/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "canonical.hpp"

#include <array>
#include <charconv>
#include <limits>
#include <system_error>

namespace agent {

namespace {

// __extension__ keeps -Wpedantic quiet about the 128-bit integers of the compiler.
__extension__ typedef unsigned __int128 Uint128;
__extension__ typedef __int128 Int128;

// The names of the header kinds, indexed by their wire code minus 1.
constexpr std::array<std::string_view, 15> kKindNames = {"raw",    "string", "bool",    "int8",    "int16",
                                                         "int32",  "int64",  "int128",  "uint8",   "uint16",
                                                         "uint32", "uint64", "uint128", "float32", "float64"};

enum class Kind : std::uint8_t {
    Raw = 1,
    String,
    Bool,
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    Uint8,
    Uint16,
    Uint32,
    Uint64,
    Uint128,
    Float32,
    Float64,
};

// The size of a value of a fixed-width kind, zero for raw bytes and strings.
std::size_t KindSize(Kind kind) {
    switch (kind) {
        case Kind::Bool:
        case Kind::Int8:
        case Kind::Uint8:
            return 1;
        case Kind::Int16:
        case Kind::Uint16:
            return 2;
        case Kind::Int32:
        case Kind::Uint32:
        case Kind::Float32:
            return 4;
        case Kind::Int64:
        case Kind::Uint64:
        case Kind::Float64:
            return 8;
        case Kind::Int128:
        case Kind::Uint128:
            return 16;
        default:
            return 0;
    }
}

Uint128 LittleEndian(const std::uint8_t *bytes, std::size_t size) {
    Uint128 value = 0;
    for (std::size_t index = size; index > 0; --index) {
        value = (value << 8) | bytes[index - 1];
    }
    return value;
}

std::vector<std::uint8_t> LittleEndianBytes(Uint128 value, std::size_t size) {
    std::vector<std::uint8_t> bytes(size);
    for (auto &byte : bytes) {
        byte = static_cast<std::uint8_t>(value);
        value >>= 8;
    }
    return bytes;
}

std::string Decimal(Uint128 value) {
    std::string digits;
    do {
        digits.insert(digits.begin(), static_cast<char>('0' + static_cast<int>(value % 10)));
        value /= 10;
    } while (value != 0);
    return digits;
}

// The two's complement value of the lowest `size` bytes, as decimal text.
std::string SignedDecimal(Uint128 bits, std::size_t size) {
    const auto width = size * 8;
    const auto sign  = static_cast<Uint128>(1) << (width - 1);
    if ((bits & sign) == 0) {
        return Decimal(bits);
    }
    const auto modulus = width == 128 ? Uint128{0} : static_cast<Uint128>(1) << width;
    return "-" + Decimal(modulus - bits);
}

std::string HexDigits(std::uint64_t value, int width) {
    static constexpr char kDigits[] = "0123456789abcdef";
    std::string text(static_cast<std::size_t>(width), '0');
    for (int index = width - 1; index >= 0; --index) {
        text[static_cast<std::size_t>(index)] = kDigits[value & 0xF];
        value >>= 4;
    }
    return text;
}

[[noreturn]] void CannotRead(std::string_view kind, std::string_view text) {
    throw AgentFailure("arguments: \"" + std::string(text) + "\" is not a " + std::string(kind) + " value");
}

// Decimal text of a two's complement value of `size` bytes, as its bits.
Uint128 ParseInteger(std::string_view text, std::size_t size, bool is_signed, std::string_view kind) {
    const bool negative = is_signed && !text.empty() && text.front() == '-';
    const auto digits   = negative ? text.substr(1) : text;
    if (digits.empty()) {
        CannotRead(kind, text);
    }
    const auto width  = size * 8;
    const auto top    = width == 128 ? ~Uint128{0} : (static_cast<Uint128>(1) << width) - 1;
    const auto limit  = is_signed ? (top >> 1) + (negative ? 1 : 0) : top;
    Uint128 magnitude = 0;
    for (const char digit : digits) {
        if (digit < '0' || digit > '9') {
            CannotRead(kind, text);
        }
        const auto value = static_cast<Uint128>(digit - '0');
        if (magnitude > (limit - value) / 10) {
            CannotRead(kind, text);
        }
        magnitude = magnitude * 10 + value;
    }
    return negative ? (Uint128{0} - magnitude) & top : magnitude;
}

std::uint64_t ParseHex(std::string_view text, std::string_view kind) {
    std::uint64_t value = 0;
    const auto end      = text.data() + text.size();
    const auto parsed   = std::from_chars(text.data(), end, value, 16);
    if (text.empty() || parsed.ec != std::errc() || parsed.ptr != end) {
        CannotRead(kind, text);
    }
    return value;
}

std::vector<std::uint8_t> FromBase64(std::string_view text) {
    static constexpr std::string_view kAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    if (text.size() % 4 != 0) {
        CannotRead("raw", text);
    }
    std::vector<std::uint8_t> bytes;
    std::uint32_t chunk = 0;
    std::size_t padding = 0;
    for (std::size_t index = 0; index < text.size(); ++index) {
        const auto position = kAlphabet.find(text[index]);
        if (text[index] == '=' && index + 2 >= text.size()) {
            ++padding;
        } else if (position == std::string_view::npos || padding > 0) {
            CannotRead("raw", text);
        }
        chunk = (chunk << 6) | (position == std::string_view::npos ? 0 : static_cast<std::uint32_t>(position));
        if (index % 4 == 3) {
            bytes.push_back(static_cast<std::uint8_t>(chunk >> 16));
            bytes.push_back(static_cast<std::uint8_t>(chunk >> 8));
            bytes.push_back(static_cast<std::uint8_t>(chunk));
            chunk = 0;
        }
    }
    bytes.resize(bytes.size() - padding);
    return bytes;
}

template <typename Stream>
Json StreamFields(const Stream &stream) {
    return {
        {"id", stream.id},
        {"name", std::string(stream.name)},
        {"created_at", stream.created_at},
        {"size_bytes", stream.size_bytes},
        {"messages_count", stream.messages_count},
        {"topics_count", stream.topics_count},
        {"options", OptionsJson(stream.options, stream.derived_options)},
    };
}

template <typename Topic>
Json TopicFields(const Topic &topic) {
    return {
        {"id", topic.id},
        {"name", std::string(topic.name)},
        {"created_at", topic.created_at},
        {"size_bytes", topic.size_bytes},
        {"messages_count", topic.messages_count},
        {"partitions_count", topic.partitions_count},
        {"options", OptionsJson(topic.options, topic.derived_options)},
    };
}

Json PartitionJson(const ffi::Partition &partition) {
    return {
        {"id", partition.id},
        {"created_at", partition.created_at},
        {"segments_count", partition.segments_count},
        {"current_offset", partition.current_offset},
        {"size_bytes", partition.size_bytes},
        {"messages_count", partition.messages_count},
    };
}

}  // namespace

Json StreamJson(const ffi::Stream &stream) {
    return StreamFields(stream);
}

Json StreamDetailsJson(const ffi::StreamDetails &stream) {
    auto topics = Json::array();
    for (const auto &topic : stream.topics) {
        topics.push_back(TopicJson(topic));
    }
    auto view      = StreamFields(stream);
    view["topics"] = std::move(topics);
    return view;
}

Json TopicJson(const ffi::Topic &topic) {
    return TopicFields(topic);
}

Json TopicDetailsJson(const ffi::TopicDetails &topic) {
    auto partitions = Json::array();
    for (const auto &partition : topic.partitions) {
        partitions.push_back(PartitionJson(partition));
    }
    auto view          = TopicFields(topic);
    view["partitions"] = std::move(partitions);
    return view;
}

Json PolledJson(const ffi::PolledMessages &polled) {
    auto messages = Json::array();
    for (const auto &message : polled.messages) {
        messages.push_back(MessageJson(message));
    }
    return {
        {"partition_id", polled.partition_id},
        {"current_offset", polled.current_offset},
        {"count", polled.count},
        {"messages", std::move(messages)},
    };
}

Json MessageJson(const ffi::IggyMessagePolled &message) {
    Json view = {
        {"offset", message.offset},       {"id", Decimal128(message.id_hi, message.id_lo)},
        {"timestamp", message.timestamp}, {"origin_timestamp", message.origin_timestamp},
        {"checksum", message.checksum},   {"payload_hex", Hex(message.payload)},
    };
    if (IsUtf8(message.payload.data(), message.payload.size())) {
        view["payload"] = std::string(message.payload.begin(), message.payload.end());
    }
    if (!message.user_headers.empty()) {
        auto headers = Json::object();
        for (const auto &entry : message.user_headers) {
            headers[KeyText(entry.key)] = {{"kind", KindName(entry.value.kind)}, {"value", ValueText(entry.value)}};
        }
        view["headers"] = std::move(headers);
    }
    return view;
}

Json GroupJson(const ffi::ConsumerGroupDetails &group) {
    auto members = Json::array();
    for (const auto &member : group.members) {
        Json view = {
            {"id", member.id},
            {"partitions_count", member.partitions_count},
            {"partitions", std::vector<std::uint32_t>(member.partitions.begin(), member.partitions.end())},
        };
        members.push_back(std::move(view));
    }
    return {
        {"id", group.id},
        {"name", std::string(group.name)},
        {"partitions_count", group.partitions_count},
        {"members_count", group.members_count},
        {"members", std::move(members)},
    };
}

Json OptionSpecJson(const ffi::OptionSpec &spec) {
    return {
        {"key", std::string(spec.key)},  {"kind", KindName(spec.kind)}, {"updatable", spec.updatable},
        {"resettable", spec.resettable}, {"secret", spec.secret},       {"read_only", spec.read_only},
        {"critical", spec.critical},
    };
}

Json UserJson(const ffi::UserInfoDetails &user) {
    Json permissions;
    if (user.has_permissions) {
        permissions = {{"global", GlobalPermissionsJson(user.permissions.global)}};
    }
    return {
        {"id", user.id},
        {"username", std::string(user.username)},
        {"status", StatusName(user.status)},
        {"created_at", user.created_at},
        {"permissions", std::move(permissions)},
        {"options", OptionsJson(user.options, user.derived_options)},
    };
}

Json GlobalPermissionsJson(const ffi::GlobalPermissions &global) {
    return {
        {"manage_servers", global.manage_servers}, {"read_servers", global.read_servers},
        {"manage_users", global.manage_users},     {"read_users", global.read_users},
        {"manage_streams", global.manage_streams}, {"read_streams", global.read_streams},
        {"manage_topics", global.manage_topics},   {"read_topics", global.read_topics},
        {"poll_messages", global.poll_messages},   {"send_messages", global.send_messages},
    };
}

std::string StatusName(ffi::UserStatus status) {
    switch (status) {
        case ffi::UserStatus::Active:
            return "active";
        case ffi::UserStatus::Inactive:
            return "inactive";
    }
    throw AgentFailure("result: an unknown user status " + std::to_string(static_cast<int>(status)));
}

Json OptionsJson(const rust::Vec<ffi::HeaderEntry> &explicit_entries, const rust::Vec<ffi::HeaderEntry> &derived) {
    auto options   = Json::object();
    const auto add = [&options](const rust::Vec<ffi::HeaderEntry> &entries, bool is_explicit) {
        for (const auto &entry : entries) {
            options[KeyText(entry.key)] = {
                {"value", ValueText(entry.value)},
                {"kind", KindName(entry.value.kind)},
                {"explicit", is_explicit},
            };
        }
    };
    add(derived, false);
    add(explicit_entries, true);
    return options;
}

std::string KindName(std::uint8_t kind) {
    if (kind == 0 || kind > kKindNames.size()) {
        throw AgentFailure("result: an unknown header kind " + std::to_string(kind));
    }
    return std::string(kKindNames[kind - 1]);
}

std::uint8_t KindCode(std::string_view name) {
    for (std::size_t index = 0; index < kKindNames.size(); ++index) {
        if (kKindNames[index] == name) {
            return static_cast<std::uint8_t>(index + 1);
        }
    }
    throw AgentFailure("arguments: an unknown header kind " + std::string(name));
}

std::string ValueText(const ffi::HeaderField &field) {
    const auto kind   = static_cast<Kind>(field.kind);
    const auto *bytes = field.value.data();
    const auto size   = field.value.size();
    const auto width  = KindSize(kind);
    if (width != 0 && size != width) {
        throw AgentFailure("result: a " + KindName(field.kind) + " value of " + std::to_string(size) + " bytes");
    }
    switch (kind) {
        case Kind::Raw:
            return Base64(bytes, size);
        case Kind::String:
            if (!IsUtf8(bytes, size)) {
                throw AgentFailure("result: a string value that is not UTF-8");
            }
            return std::string(field.value.begin(), field.value.end());
        case Kind::Bool:
            if (bytes[0] > 1) {
                throw AgentFailure("result: a bool value of " + std::to_string(bytes[0]));
            }
            return bytes[0] == 1 ? "true" : "false";
        case Kind::Int8:
        case Kind::Int16:
        case Kind::Int32:
        case Kind::Int64:
        case Kind::Int128:
            return SignedDecimal(LittleEndian(bytes, size), size);
        case Kind::Uint8:
        case Kind::Uint16:
        case Kind::Uint32:
        case Kind::Uint64:
        case Kind::Uint128:
            return Decimal(LittleEndian(bytes, size));
        case Kind::Float32:
        case Kind::Float64:
            return "0x" + HexDigits(static_cast<std::uint64_t>(LittleEndian(bytes, size)), static_cast<int>(size * 2));
    }
    throw AgentFailure("result: an unknown header kind " + std::to_string(field.kind));
}

std::vector<std::uint8_t> ValueBytes(std::uint8_t code, std::string_view text) {
    const auto kind      = static_cast<Kind>(code);
    const auto size      = KindSize(kind);
    const auto kind_name = KindName(code);
    switch (kind) {
        case Kind::Raw:
            return FromBase64(text);
        case Kind::String:
            return {text.begin(), text.end()};
        case Kind::Bool:
            if (text != "true" && text != "false") {
                CannotRead(kind_name, text);
            }
            return {static_cast<std::uint8_t>(text == "true" ? 1 : 0)};
        case Kind::Int8:
        case Kind::Int16:
        case Kind::Int32:
        case Kind::Int64:
        case Kind::Int128:
            return LittleEndianBytes(ParseInteger(text, size, true, kind_name), size);
        case Kind::Uint8:
        case Kind::Uint16:
        case Kind::Uint32:
        case Kind::Uint64:
        case Kind::Uint128:
            return LittleEndianBytes(ParseInteger(text, size, false, kind_name), size);
        case Kind::Float32:
        case Kind::Float64:
            if (text.size() != 2 + size * 2 || text.substr(0, 2) != "0x") {
                CannotRead(kind_name, text);
            }
            return LittleEndianBytes(ParseHex(text.substr(2), kind_name), size);
    }
    CannotRead(kind_name, text);
}

std::string KeyText(const ffi::HeaderField &key) {
    if (!IsUtf8(key.value.data(), key.value.size())) {
        throw AgentFailure("result: a key that is not UTF-8");
    }
    return std::string(key.value.begin(), key.value.end());
}

std::string Hex(const std::uint8_t *data, std::size_t size) {
    static constexpr char kDigits[] = "0123456789abcdef";
    std::string text;
    text.reserve(size * 2);
    for (std::size_t index = 0; index < size; ++index) {
        text.push_back(kDigits[data[index] >> 4]);
        text.push_back(kDigits[data[index] & 0xF]);
    }
    return text;
}

std::vector<std::uint8_t> FromHex(std::string_view text) {
    if (text.size() % 2 != 0) {
        throw AgentFailure("arguments: hex text of odd length");
    }
    std::vector<std::uint8_t> bytes;
    bytes.reserve(text.size() / 2);
    for (std::size_t index = 0; index < text.size(); index += 2) {
        bytes.push_back(static_cast<std::uint8_t>(ParseHex(text.substr(index, 2), "hex")));
    }
    return bytes;
}

std::string Base64(const std::uint8_t *data, std::size_t size) {
    static constexpr char kAlphabet[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string text;
    text.reserve((size + 2) / 3 * 4);
    for (std::size_t index = 0; index < size; index += 3) {
        const auto remaining = size - index;
        std::uint32_t chunk  = static_cast<std::uint32_t>(data[index]) << 16;
        if (remaining > 1) {
            chunk |= static_cast<std::uint32_t>(data[index + 1]) << 8;
        }
        if (remaining > 2) {
            chunk |= data[index + 2];
        }
        text.push_back(kAlphabet[(chunk >> 18) & 0x3F]);
        text.push_back(kAlphabet[(chunk >> 12) & 0x3F]);
        text.push_back(remaining > 1 ? kAlphabet[(chunk >> 6) & 0x3F] : '=');
        text.push_back(remaining > 2 ? kAlphabet[chunk & 0x3F] : '=');
    }
    return text;
}

// Well-formed UTF-8 as Unicode Table 3-7 defines it, which Rust's str::from_utf8 also accepts.
bool IsUtf8(const std::uint8_t *data, std::size_t size) {
    std::size_t index = 0;
    while (index < size) {
        const auto lead = data[index];
        if (lead < 0x80) {
            ++index;
            continue;
        }
        std::size_t continuation = 0;
        std::uint8_t low         = 0x80;
        std::uint8_t high        = 0xBF;
        if (lead >= 0xC2 && lead <= 0xDF) {
            continuation = 1;
        } else if (lead >= 0xE0 && lead <= 0xEF) {
            continuation = 2;
            low          = lead == 0xE0 ? 0xA0 : 0x80;
            high         = lead == 0xED ? 0x9F : 0xBF;
        } else if (lead >= 0xF0 && lead <= 0xF4) {
            continuation = 3;
            low          = lead == 0xF0 ? 0x90 : 0x80;
            high         = lead == 0xF4 ? 0x8F : 0xBF;
        } else {
            return false;
        }
        if (size - index <= continuation) {
            return false;
        }
        for (std::size_t offset = 1; offset <= continuation; ++offset) {
            const auto byte = data[index + offset];
            if (byte < (offset == 1 ? low : 0x80) || byte > (offset == 1 ? high : 0xBF)) {
                return false;
            }
        }
        index += continuation + 1;
    }
    return true;
}

std::string Hex128(std::uint64_t high, std::uint64_t low) {
    return HexDigits(high, 16) + HexDigits(low, 16);
}

std::string Decimal128(std::uint64_t high, std::uint64_t low) {
    return Decimal((static_cast<Uint128>(high) << 64) | low);
}

std::pair<std::uint64_t, std::uint64_t> ParseDecimal128(std::string_view text) {
    const auto value = ParseInteger(text, 16, false, "u128");
    return {static_cast<std::uint64_t>(value >> 64), static_cast<std::uint64_t>(value)};
}

}  // namespace agent
