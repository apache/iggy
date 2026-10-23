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

// Unique step-class prefix so the generated CukeObject<n> symbols do not clash with the other
// step files when linked together (see background_steps.cpp for details).
#define CUKE_OBJECT_PREFIX IggyBddResourceOptions

#include <gtest/gtest.h>

#include <cucumber-cpp/autodetect.hpp>

#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "world.hpp"

namespace {

constexpr std::size_t kKeyLength      = 32;
constexpr std::uint32_t kPartitionId  = 0;
constexpr std::size_t kFingerprintHex = 16;

constexpr iggy::StreamOptionKey kStreamOptionKeys[] = {
    iggy::StreamOptionKey::Name,
    iggy::StreamOptionKey::EncryptionKey,
    iggy::StreamOptionKey::EncryptionKeySource,
    iggy::StreamOptionKey::EncryptionKeyFingerprint,
    iggy::StreamOptionKey::EncryptionKekFingerprint,
};

constexpr iggy::TopicOptionKey kTopicOptionKeys[] = {
    iggy::TopicOptionKey::Name,
    iggy::TopicOptionKey::CompressionAlgorithm,
    iggy::TopicOptionKey::MessageExpiry,
    iggy::TopicOptionKey::MaxTopicSize,
    iggy::TopicOptionKey::SegmentSize,
    iggy::TopicOptionKey::Durability,
    iggy::TopicOptionKey::ConsumerOffsetDurability,
    iggy::TopicOptionKey::PartitionResizePolicy,
    iggy::TopicOptionKey::MessagesRequiredToSave,
    iggy::TopicOptionKey::SizeOfMessagesRequiredToSave,
    iggy::TopicOptionKey::PreallocateSegments,
};

iggy::IggyBlockingClient &Sdk(bdd::GlobalContext &context) {
    if (!context.sdk) {
        auto client =
            iggy::IggyBlockingClient::Builder()
                .WithServerAddress(bdd::required_env("IGGY_TCP_ADDRESS"))
                .WithAutoLogin(bdd::required_env("IGGY_ROOT_USERNAME"), bdd::required_env("IGGY_ROOT_PASSWORD"))
                .Build();
        client.Connect();
        context.sdk.emplace(std::move(client));
    }
    return *context.sdk;
}

template <typename Key, std::size_t Count>
Key KeyFromName(const Key (&keys)[Count], const std::string &name) {
    for (const auto key : keys) {
        if (iggy::to_string(key) == name) {
            return key;
        }
    }
    throw std::invalid_argument("Unknown option key " + name);
}

iggy::UserStatus UserStatusFromName(const std::string &name) {
    if (name == "active") {
        return iggy::UserStatus::Active;
    }
    if (name == "inactive") {
        return iggy::UserStatus::Inactive;
    }
    throw std::invalid_argument("Unknown user status " + name);
}

iggy::Identifier StreamIdentifier(const bdd::GlobalContext &context) {
    return iggy::Identifier::Numeric(context.last_stream_id.value());
}

iggy::Identifier TopicIdentifier(const bdd::GlobalContext &context) {
    return iggy::Identifier::Numeric(context.last_topic_id.value());
}

iggy::StreamDetails FetchStream(bdd::GlobalContext &context) {
    return Sdk(context).GetStream(StreamIdentifier(context));
}

iggy::TopicDetails FetchTopic(bdd::GlobalContext &context) {
    return Sdk(context).GetTopic(StreamIdentifier(context), TopicIdentifier(context));
}

iggy::UserInfoDetails FetchUser(bdd::GlobalContext &context) {
    return Sdk(context).GetUser(iggy::Identifier::Numeric(context.last_user_id.value()));
}

void CreateStream(bdd::GlobalContext &context,
                  const std::string &name,
                  const std::optional<std::vector<std::uint8_t>> &key) {
    iggy::StreamCreateOptions options;
    if (key) {
        options.SetEncryptionKey(*key);
    }
    const auto stream       = Sdk(context).CreateStream(name, options);
    context.last_stream_id  = stream.Id();
    context.last_stream_key = key;
}

void CreateTopic(bdd::GlobalContext &context, const std::string &name, iggy::TopicCreateOptions options) {
    const auto topic      = Sdk(context).CreateTopic(StreamIdentifier(context), name, options);
    context.last_topic_id = topic.Id();
}

iggy::ffi::HeaderField StringField(const std::string &text) {
    iggy::ffi::HeaderField field;
    field.kind  = static_cast<std::uint8_t>(iggy::ffi::HeaderKind::String);
    field.value = bdd::to_payload(text);
    return field;
}

void SendMessage(bdd::GlobalContext &context, const std::string &payload, rust::Vec<iggy::ffi::HeaderEntry> headers) {
    iggy::ffi::ErrorInfo error_info{};
    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    messages.push_back(iggy::ffi::make_message(bdd::to_payload(payload), std::move(headers)));
    context.client->send_messages(bdd::make_numeric_identifier(context.last_stream_id.value()),
                                  bdd::make_numeric_identifier(context.last_topic_id.value()), "partition_id",
                                  bdd::partition_id_bytes(kPartitionId), std::move(messages), error_info);
}

iggy::ffi::IggyMessagePolled PollFirst(bdd::GlobalContext &context) {
    iggy::ffi::ErrorInfo error_info{};
    auto polled =
        context.client->poll_messages(bdd::make_numeric_identifier(context.last_stream_id.value()),
                                      bdd::make_numeric_identifier(context.last_topic_id.value()), kPartitionId,
                                      "consumer", bdd::make_numeric_identifier(1), "offset", 0, 1, false, error_info);
    if (polled.messages.empty()) {
        throw std::runtime_error("No message was polled");
    }
    return std::move(polled.messages[0]);
}

const iggy::OptionSpec &StreamOptionSpec(const bdd::GlobalContext &context, const std::string &key) {
    for (const auto &spec : context.last_option_specs) {
        if (spec.Key() == key) {
            return spec;
        }
    }
    throw std::runtime_error("Stream option " + key + " was not described");
}

void UpdateTopic(bdd::GlobalContext &context, const iggy::TopicUpdateOptions &options) {
    Sdk(context).UpdateTopic(StreamIdentifier(context), TopicIdentifier(context), options);
}

}  // namespace

WHEN("^I create a stream named \"([^\"]+)\" with an encryption key of 32 bytes valued ([0-9]+)$") {
    REGEX_PARAM(std::string, name);
    REGEX_PARAM(int, byte);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    CreateStream(*context, name, std::vector<std::uint8_t>(kKeyLength, static_cast<std::uint8_t>(byte)));
}

GIVEN("^a stream named \"([^\"]+)\" with an encryption key of 32 bytes valued ([0-9]+) exists$") {
    REGEX_PARAM(std::string, name);
    REGEX_PARAM(int, byte);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    CreateStream(*context, name, std::vector<std::uint8_t>(kKeyLength, static_cast<std::uint8_t>(byte)));
}

WHEN("^I create a stream named \"([^\"]+)\" without an encryption key$") {
    REGEX_PARAM(std::string, name);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    CreateStream(*context, name, std::nullopt);
}

GIVEN("^a stream named \"([^\"]+)\" exists$") {
    REGEX_PARAM(std::string, name);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    CreateStream(*context, name, std::nullopt);
}

THEN("^the stream should report encryption key source \"([^\"]+)\"$") {
    REGEX_PARAM(std::string, source);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    const auto stream = FetchStream(*context);
    ASSERT_TRUE(stream.EncryptionKeySource().has_value());
    EXPECT_EQ(iggy::to_string(*stream.EncryptionKeySource()), source);
}

THEN("^the stream encryption key fingerprint should match the key$") {
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    ASSERT_TRUE(context->last_stream_key.has_value());
    const auto stream = FetchStream(*context);
    EXPECT_EQ(stream.EncryptionKeyFingerprint(), iggy::EncryptionKeyFingerprint(*context->last_stream_key));
}

THEN("^the stream should report an encryption key fingerprint$") {
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    const auto stream = FetchStream(*context);
    ASSERT_TRUE(stream.EncryptionKeyFingerprint().has_value());
    EXPECT_EQ(stream.EncryptionKeyFingerprint()->size(), kFingerprintHex);
}

THEN("^the stream should report at least one KEK fingerprint$") {
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    EXPECT_FALSE(FetchStream(*context).EncryptionKekFingerprints().empty());
}

THEN("^the stream should expose no encryption key material$") {
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    const auto stream = FetchStream(*context);
    for (const auto *secret : {"encryption_key", "encryption_key_wrapped"}) {
        EXPECT_EQ(stream.Options().Explicit().count(secret), 0u) << secret << " must never be returned";
        EXPECT_EQ(stream.Options().Derived().count(secret), 0u) << secret << " must never be returned";
    }
}

WHEN("^I create a topic named \"([^\"]+)\" with ([0-9]+) partitions? in the stream$") {
    REGEX_PARAM(std::string, name);
    REGEX_PARAM(int, partitions_count);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    CreateTopic(*context, name,
                iggy::TopicCreateOptions()
                    .SetPartitionsCount(static_cast<std::uint32_t>(partitions_count))
                    .SetMessageExpiry(iggy::Expiry::NeverExpire()));
}

WHEN("^I send a message with payload \"([^\"]+)\" to the topic$") {
    REGEX_PARAM(std::string, payload);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    SendMessage(*context, payload, rust::Vec<iggy::ffi::HeaderEntry>());
}

WHEN("^I send a message with payload \"([^\"]+)\" and header \"([^\"]+)\" set to \"([^\"]+)\" to the topic$") {
    REGEX_PARAM(std::string, payload);
    REGEX_PARAM(std::string, header);
    REGEX_PARAM(std::string, value);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    rust::Vec<iggy::ffi::HeaderEntry> headers;
    iggy::ffi::HeaderEntry entry;
    entry.key   = StringField(header);
    entry.value = StringField(value);
    headers.push_back(std::move(entry));
    SendMessage(*context, payload, std::move(headers));
}

THEN("^polling the topic should return the payload \"([^\"]+)\"$") {
    REGEX_PARAM(std::string, payload);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    EXPECT_EQ(bdd::payload_to_string(PollFirst(*context).payload), payload);
}

THEN("^polling the topic should return the payload \"([^\"]+)\" and header \"([^\"]+)\" set to \"([^\"]+)\"$") {
    REGEX_PARAM(std::string, payload);
    REGEX_PARAM(std::string, header);
    REGEX_PARAM(std::string, value);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    const auto message = PollFirst(*context);
    EXPECT_EQ(bdd::payload_to_string(message.payload), payload);
    bool found = false;
    for (const auto &entry : message.user_headers) {
        if (entry.key.kind == static_cast<std::uint8_t>(iggy::ffi::HeaderKind::String) &&
            bdd::payload_to_string(entry.key.value) == header) {
            found = true;
            EXPECT_EQ(entry.value.kind, static_cast<std::uint8_t>(iggy::ffi::HeaderKind::String));
            EXPECT_EQ(bdd::payload_to_string(entry.value.value), value);
        }
    }
    EXPECT_TRUE(found) << "header " << header << " is missing";
}

WHEN("^I rename the stream to \"([^\"]+)\"$") {
    REGEX_PARAM(std::string, name);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    Sdk(*context).UpdateStream(StreamIdentifier(*context), iggy::StreamUpdateOptions().SetName(name));
}

THEN("^the stream name should be \"([^\"]+)\"$") {
    REGEX_PARAM(std::string, name);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    EXPECT_EQ(FetchStream(*context).Name(), name);
}

WHEN("^I request a reset of the stream option \"([^\"]+)\"$") {
    REGEX_PARAM(std::string, key);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    const auto options = iggy::StreamUpdateOptions().Reset(KeyFromName(kStreamOptionKeys, key));
    try {
        Sdk(*context).UpdateStream(StreamIdentifier(*context), options);
        context->last_update_rejected = false;
    } catch (const iggy::IggyException &) {
        context->last_update_rejected = true;
    }
}

THEN("^the stream update should be rejected$") {
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    EXPECT_TRUE(context->last_update_rejected) << "the update must fail";
}

WHEN("^I describe the stream options$") {
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    context->last_option_specs = Sdk(*context).DescribeOptions(iggy::OptionsScope::Stream);
}

THEN("^the stream option \"([^\"]+)\" should be updatable and not secret$") {
    REGEX_PARAM(std::string, key);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    const auto &spec = StreamOptionSpec(*context, key);
    EXPECT_TRUE(spec.Updatable() && !spec.Secret() && !spec.ReadOnly());
}

THEN("^the stream option \"([^\"]+)\" should be create only and secret$") {
    REGEX_PARAM(std::string, key);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    const auto &spec = StreamOptionSpec(*context, key);
    EXPECT_TRUE(!spec.Updatable() && !spec.Resettable() && spec.Secret() && !spec.ReadOnly());
}

THEN("^the stream option \"([^\"]+)\" should be read only$") {
    REGEX_PARAM(std::string, key);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    const auto &spec = StreamOptionSpec(*context, key);
    EXPECT_TRUE(spec.ReadOnly() && !spec.Updatable() && !spec.Secret());
}

GIVEN(
    "^a topic named \"([^\"]+)\" exists in the stream with message expiry ([0-9]+) microseconds and max topic "
    "size ([0-9]+) bytes$") {
    REGEX_PARAM(std::string, name);
    REGEX_PARAM(std::uint64_t, expiry);
    REGEX_PARAM(std::uint64_t, max_size);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    CreateTopic(*context, name,
                iggy::TopicCreateOptions()
                    .SetPartitionsCount(1)
                    .SetMessageExpiry(iggy::Expiry::Duration(expiry))
                    .SetMaxTopicSize(iggy::MaxTopicSize::FromBytes(max_size)));
}

WHEN("^I update the topic message expiry to ([0-9]+) microseconds without a name$") {
    REGEX_PARAM(std::uint64_t, expiry);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    UpdateTopic(*context, iggy::TopicUpdateOptions().SetMessageExpiry(iggy::Expiry::Duration(expiry)));
}

WHEN("^I reset the topic option \"([^\"]+)\"$") {
    REGEX_PARAM(std::string, key);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    UpdateTopic(*context, iggy::TopicUpdateOptions().Reset(KeyFromName(kTopicOptionKeys, key)));
}

WHEN("^I rename the topic to \"([^\"]+)\"$") {
    REGEX_PARAM(std::string, name);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    UpdateTopic(*context, iggy::TopicUpdateOptions().SetName(name));
}

THEN("^the topic name should be \"([^\"]+)\"$") {
    REGEX_PARAM(std::string, name);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    EXPECT_EQ(FetchTopic(*context).Name(), name);
}

THEN("^the topic message expiry should be ([0-9]+) microseconds set explicitly$") {
    REGEX_PARAM(std::uint64_t, expiry);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    const auto topic = FetchTopic(*context);
    EXPECT_EQ(topic.MessageExpiry(), expiry);
    EXPECT_EQ(topic.Options().Explicit().count(std::string(iggy::to_string(iggy::TopicOptionKey::MessageExpiry))), 1u);
}

THEN("^the topic max topic size should be ([0-9]+) bytes set explicitly$") {
    REGEX_PARAM(std::uint64_t, max_size);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    const auto topic = FetchTopic(*context);
    EXPECT_EQ(topic.MaxTopicSize(), max_size);
    EXPECT_EQ(topic.Options().Explicit().count(std::string(iggy::to_string(iggy::TopicOptionKey::MaxTopicSize))), 1u);
}

THEN("^the topic max topic size should be derived from the server default$") {
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    const auto topic = FetchTopic(*context);
    const auto key   = std::string(iggy::to_string(iggy::TopicOptionKey::MaxTopicSize));
    EXPECT_EQ(topic.Options().Explicit().count(key), 0u);
    EXPECT_EQ(topic.Options().Derived().count(key), 1u);
    EXPECT_EQ(topic.MaxTopicSize(), std::numeric_limits<std::uint64_t>::max());
}

GIVEN("^a user named \"([^\"]+)\" with password \"([^\"]+)\" exists$") {
    REGEX_PARAM(std::string, username);
    REGEX_PARAM(std::string, password);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    const auto user =
        Sdk(*context).CreateUser(username, password, iggy::UserStatus::Active, std::nullopt, iggy::UserCreateOptions());
    context->last_user_id = user.Id();
}

WHEN("^I update the user username to \"([^\"]+)\" and status to \"([^\"]+)\"$") {
    REGEX_PARAM(std::string, username);
    REGEX_PARAM(std::string, status);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    Sdk(*context).UpdateUser(iggy::Identifier::Numeric(context->last_user_id.value()),
                             iggy::UserUpdateOptions().SetUsername(username).SetStatus(UserStatusFromName(status)));
}

THEN("^the user should have username \"([^\"]+)\"$") {
    REGEX_PARAM(std::string, username);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    EXPECT_EQ(FetchUser(*context).Username(), username);
}

THEN("^the user should have status \"([^\"]+)\"$") {
    REGEX_PARAM(std::string, status);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    EXPECT_EQ(FetchUser(*context).Status(), UserStatusFromName(status));
}
