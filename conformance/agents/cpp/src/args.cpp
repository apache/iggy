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

#include "args.hpp"

#include <array>
#include <limits>
#include <utility>

#include "canonical.hpp"

namespace agent {

namespace {

const Json &NoArgs() {
    static const Json kNoArgs = Json::object();
    return kNoArgs;
}

[[noreturn]] void Refuse(std::string_view what, const Json &value) {
    throw AgentFailure("arguments: " + std::string(what) + " cannot be " + value.dump());
}

// A one-key object such as {"offset": 3}, or a bare keyword such as "first", which has no value.
std::pair<std::string, const Json *> Keyed(const Json &value, std::string_view what) {
    if (value.is_string()) {
        return {value.get<std::string>(), nullptr};
    }
    if (!value.is_object() || value.size() != 1) {
        Refuse(what, value);
    }
    return {value.begin().key(), &value.begin().value()};
}

std::vector<std::uint8_t> TextBytes(const std::string &text) {
    return {text.begin(), text.end()};
}

ffi::IggyMessageToSend Message(const Json &value) {
    Args args(value);
    const auto *id      = args.Optional("id");
    const auto *payload = args.Optional("payload");
    const auto *hex     = args.Optional("payload_hex");
    const auto *headers = args.Optional("headers");
    args.Finish();
    if ((payload == nullptr) == (hex == nullptr)) {
        throw AgentFailure("arguments: a message needs exactly one of payload and payload_hex");
    }
    const auto bytes = payload != nullptr ? TextBytes(Text(*payload, "payload")) : FromHex(Text(*hex, "payload_hex"));

    rust::Vec<ffi::HeaderEntry> entries;
    if (headers != nullptr) {
        if (!headers->is_object()) {
            Refuse("headers", *headers);
        }
        for (const auto &[key, header] : headers->items()) {
            Args fields(header);
            const auto kind = KindCode(Text(fields.Required("kind"), "header kind"));
            const auto text = Text(fields.Required("value"), "header value");
            fields.Finish();
            ffi::HeaderEntry entry{};
            entry.key.kind    = KindCode("string");
            entry.key.value   = RustBytes(TextBytes(key));
            entry.value.kind  = kind;
            entry.value.value = RustBytes(ValueBytes(kind, text));
            entries.push_back(std::move(entry));
        }
    }
    auto message = ffi::make_message(RustBytes(bytes), std::move(entries));
    if (id != nullptr) {
        const auto [high, low] = ParseDecimal128(Text(*id, "message id"));
        message.id_hi          = high;
        message.id_lo          = low;
    }
    return message;
}

}  // namespace

Args::Args(const Json &value) : value_(value.is_null() ? NoArgs() : value) {
    if (!value_.is_object()) {
        Refuse("the arguments", value_);
    }
}

const Json *Args::Optional(std::string_view key) {
    taken_.emplace(key);
    const auto field = value_.find(std::string(key));
    if (field == value_.end() || field->is_null()) {
        return nullptr;
    }
    return &*field;
}

const Json &Args::Required(std::string_view key) {
    const auto *field = Optional(key);
    if (field == nullptr) {
        throw AgentFailure("arguments: " + std::string(key) + " is missing");
    }
    return *field;
}

void Args::Finish() const {
    for (const auto &[key, value] : value_.items()) {
        if (taken_.find(key) == taken_.end()) {
            throw AgentFailure("arguments: unknown field " + key);
        }
    }
}

std::string Text(const Json &value, std::string_view what) {
    if (!value.is_string()) {
        Refuse(what, value);
    }
    return value.get<std::string>();
}

std::uint64_t Unsigned(const Json &value, std::string_view what) {
    if (!value.is_number_unsigned()) {
        Refuse(what, value);
    }
    return value.get<std::uint64_t>();
}

std::uint32_t Unsigned32(const Json &value, std::string_view what) {
    const auto number = Unsigned(value, what);
    if (number > std::numeric_limits<std::uint32_t>::max()) {
        Refuse(what, value);
    }
    return static_cast<std::uint32_t>(number);
}

std::uint8_t Unsigned8(const Json &value, std::string_view what) {
    const auto number = Unsigned(value, what);
    if (number > std::numeric_limits<std::uint8_t>::max()) {
        Refuse(what, value);
    }
    return static_cast<std::uint8_t>(number);
}

bool Boolean(const Json &value, std::string_view what) {
    if (!value.is_boolean()) {
        Refuse(what, value);
    }
    return value.get<bool>();
}

rust::Vec<std::uint8_t> RustBytes(const std::vector<std::uint8_t> &bytes) {
    rust::Vec<std::uint8_t> converted;
    converted.reserve(bytes.size());
    for (const auto byte : bytes) {
        converted.push_back(byte);
    }
    return converted;
}

ffi::Identifier Identifier(const Json &value) {
    if (!value.is_string()) {
        return NumericIdentifier(Unsigned32(value, "an identifier"));
    }
    const auto name = value.get<std::string>();
    ffi::Identifier identifier{};
    Sdk([&](ffi::ErrorInfo &) { identifier.set_string(name); });
    return identifier;
}

ffi::Identifier NumericIdentifier(std::uint32_t id) {
    ffi::Identifier identifier{};
    Sdk([&](ffi::ErrorInfo &) { identifier.set_numeric(id); });
    return identifier;
}

ConsumerArg Consumer(const Json *value) {
    // The SDK takes no default consumer, so the agent passes the default of the Rust SDK, the
    // consumer 0.
    if (value == nullptr) {
        return {"consumer", NumericIdentifier(0)};
    }
    const auto [kind, id] = Keyed(*value, "consumer");
    if (id == nullptr || (kind != "consumer" && kind != "group")) {
        Refuse("consumer", *value);
    }
    return {kind == "group" ? "consumer_group" : "consumer", Identifier(*id)};
}

PartitioningArg Partitioning(const Json &value) {
    const auto [kind, argument] = Keyed(value, "partitioning");
    if (kind == "balanced" && argument == nullptr) {
        return {kind, {}};
    }
    if (kind == "partition_id" && argument != nullptr) {
        const auto partition = Unsigned32(*argument, "partition_id");
        std::vector<std::uint8_t> bytes;
        for (int shift = 0; shift < 32; shift += 8) {
            bytes.push_back(static_cast<std::uint8_t>(partition >> shift));
        }
        return {kind, RustBytes(bytes)};
    }
    if (kind == "messages_key" && argument != nullptr) {
        return {kind, RustBytes(TextBytes(Text(*argument, "messages_key")))};
    }
    Refuse("partitioning", value);
}

ffi::PollingStrategy Strategy(const Json &value) {
    const auto [kind, argument] = Keyed(value, "strategy");
    ffi::PollingStrategy strategy{};
    strategy.kind = kind;
    if ((kind == "first" || kind == "last" || kind == "next") && argument == nullptr) {
        return strategy;
    }
    if ((kind == "offset" || kind == "timestamp") && argument != nullptr) {
        strategy.value = Unsigned(*argument, kind);
        return strategy;
    }
    Refuse("strategy", value);
}

void SetCommit(ffi::PollOptions &options, const Json *value) {
    const auto commit = value == nullptr ? std::string("manual") : Text(*value, "commit");
    if (commit == "manual") {
        options.commit = ffi::CommitPolicy::Manual;
    } else if (commit == "on_read") {
        options.commit = ffi::CommitPolicy::OnRead;
    } else if (commit == "after_processing") {
        // The other auto_ fields keep the SDK defaults: every 10,000 messages or 5 s.
        options.commit     = ffi::CommitPolicy::Auto;
        options.auto_after = ffi::CommitAfter::Processing;
    } else {
        Refuse("commit", *value);
    }
}

ExpiryArg Expiry(const Json &value) {
    if (value.is_number_unsigned()) {
        return {"duration", value.get<std::uint64_t>()};
    }
    const auto keyword = Text(value, "message_expiry");
    if (keyword == "never") {
        return {"never_expire", std::numeric_limits<std::uint64_t>::max()};
    }
    if (keyword == "server_default") {
        return {"server_default", 0};
    }
    Refuse("message_expiry", value);
}

std::string TopicSize(const Json &value) {
    if (value.is_number_unsigned()) {
        return std::to_string(value.get<std::uint64_t>());
    }
    const auto keyword = Text(value, "max_topic_size");
    if (keyword != "unlimited" && keyword != "server_default") {
        Refuse("max_topic_size", value);
    }
    return keyword;
}

rust::Vec<rust::String> ResetKeys(const Json *value) {
    rust::Vec<rust::String> keys;
    if (value == nullptr) {
        return keys;
    }
    if (!value->is_array()) {
        Refuse("reset", *value);
    }
    for (const auto &key : *value) {
        keys.push_back(Text(key, "reset"));
    }
    return keys;
}

ffi::UserStatus UserStatus(const Json &value) {
    const auto status = Text(value, "status");
    if (status == "active") {
        return ffi::UserStatus::Active;
    }
    if (status == "inactive") {
        return ffi::UserStatus::Inactive;
    }
    Refuse("status", value);
}

ffi::Permissions Permissions(const Json &value) {
    Args permissions(value);
    const auto *global = permissions.Optional("global");
    permissions.Finish();
    ffi::Permissions converted{};
    if (global == nullptr) {
        return converted;
    }
    Args flags(*global);
    const auto flag = [&flags](std::string_view name) {
        const auto *field = flags.Optional(name);
        return field != nullptr && Boolean(*field, name);
    };
    auto &granted          = converted.global;
    granted.manage_servers = flag("manage_servers");
    granted.read_servers   = flag("read_servers");
    granted.manage_users   = flag("manage_users");
    granted.read_users     = flag("read_users");
    granted.manage_streams = flag("manage_streams");
    granted.read_streams   = flag("read_streams");
    granted.manage_topics  = flag("manage_topics");
    granted.read_topics    = flag("read_topics");
    granted.poll_messages  = flag("poll_messages");
    granted.send_messages  = flag("send_messages");
    flags.Finish();
    return converted;
}

rust::Vec<ffi::IggyMessageToSend> Messages(const Json &value) {
    if (!value.is_array()) {
        Refuse("messages", value);
    }
    rust::Vec<ffi::IggyMessageToSend> messages;
    for (const auto &message : value) {
        messages.push_back(Message(message));
    }
    return messages;
}

void SetStart(ffi::MetadataChangesQuery &query, const Json &value) {
    const auto [kind, argument] = Keyed(value, "start");
    if (kind == "earliest" && argument == nullptr) {
        query.start = ffi::MetadataChangeStart::Earliest;
    } else if (kind == "latest" && argument == nullptr) {
        query.start = ffi::MetadataChangeStart::Latest;
    } else if (kind == "after" && argument != nullptr) {
        query.start  = ffi::MetadataChangeStart::After;
        query.cursor = Text(*argument, "cursor");
    } else if (kind == "current" && argument != nullptr) {
        // An empty page token starts a current state scan.
        query.start      = ffi::MetadataChangeStart::Current;
        query.page_token = argument->is_null() ? std::string() : Text(*argument, "page token");
    } else {
        Refuse("start", value);
    }
}

rust::Vec<std::uint8_t> Groups(const Json *value) {
    static constexpr std::array<std::string_view, 8> kGroups = {"streams",
                                                                "topics",
                                                                "partitions",
                                                                "consumer_groups",
                                                                "consumer_group_members",
                                                                "users",
                                                                "personal_access_tokens",
                                                                "sessions"};
    rust::Vec<std::uint8_t> codes;
    if (value == nullptr) {
        return codes;
    }
    if (!value->is_array()) {
        Refuse("groups", *value);
    }
    for (const auto &group : *value) {
        const auto name  = Text(group, "group");
        std::size_t code = 0;
        while (code < kGroups.size() && kGroups[code] != name) {
            ++code;
        }
        if (code == kGroups.size()) {
            Refuse("group", group);
        }
        // The wire codes of the groups start at 1.
        codes.push_back(static_cast<std::uint8_t>(code + 1));
    }
    return codes;
}

}  // namespace agent
