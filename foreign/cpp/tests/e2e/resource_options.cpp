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

#include <cstddef>
#include <cstdint>
#include <fstream>
#include <limits>
#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "lib.rs.h"
#include "tests/e2e/test_helpers.hpp"

struct ResourceOptionsTransport {
    std::string name;
    // Empty selects the TCP builder.
    std::string scheme;
    std::string address;

    [[nodiscard]] std::string ConnectionString(const std::string &username, const std::string &password) const {
        return scheme + "://" + username + ":" + password + "@" + address;
    }
};

namespace {

constexpr char kFixturePath[]         = "tests/fixtures/resource_options.json";
constexpr std::size_t kFingerprintHex = 16;
// UpdateResource carries the reset count in one byte.
constexpr std::size_t kMaxResetKeys = 255;

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

constexpr iggy::UserOptionKey kUserOptionKeys[] = {
    iggy::UserOptionKey::Username,
    iggy::UserOptionKey::Status,
};

const nlohmann::json &Fixture() {
    static const nlohmann::json fixture = [] {
        std::ifstream file(kFixturePath);
        if (!file) {
            throw std::runtime_error(std::string("Cannot open ") + kFixturePath);
        }
        return nlohmann::json::parse(file);
    }();
    return fixture;
}

const nlohmann::json &FixtureCase(const std::string &section, const std::string &name) {
    for (const auto &vector : Fixture().at(section)) {
        if (vector.at("case") == name) {
            return vector;
        }
    }
    throw std::runtime_error("No fixture case " + section + "." + name);
}

std::vector<std::uint8_t> FromHex(const std::string &hex) {
    std::vector<std::uint8_t> bytes;
    bytes.reserve(hex.size() / 2);
    for (std::size_t index = 0; index + 1 < hex.size(); index += 2) {
        bytes.push_back(static_cast<std::uint8_t>(std::stoul(hex.substr(index, 2), nullptr, 16)));
    }
    return bytes;
}

std::vector<std::uint8_t> FixtureKey() {
    return FromHex(Fixture().at("stream_key").at("key_hex").get<std::string>());
}

std::uint64_t ReadLittleEndian(const std::vector<std::uint8_t> &bytes) {
    std::uint64_t value = 0;
    for (std::size_t index = bytes.size(); index > 0; --index) {
        value = (value << 8) | bytes[index - 1];
    }
    return value;
}

std::string HeaderFieldText(const iggy::HeaderField &field) {
    const auto &bytes = field.Value();
    return std::string(bytes.begin(), bytes.end());
}

std::optional<std::string> DefaultText(const iggy::OptionSpec &spec) {
    const auto &bytes = spec.DefaultValue();
    if (bytes.empty()) {
        return std::nullopt;
    }
    switch (spec.Kind()) {
        case iggy::HeaderKind::String:
            return std::string(bytes.begin(), bytes.end());
        case iggy::HeaderKind::Bool:
            return std::string(bytes.front() == 0 ? "false" : "true");
        case iggy::HeaderKind::Uint32:
        case iggy::HeaderKind::Uint64:
            return std::to_string(ReadLittleEndian(bytes));
        default:
            return std::nullopt;
    }
}

std::uint32_t FlagsOf(const iggy::OptionSpec &spec) {
    const auto &flags = Fixture().at("constants").at("option_flags");
    std::uint32_t value{};
    value |= spec.Updatable() ? flags.at("updatable").get<std::uint32_t>() : 0;
    value |= spec.Resettable() ? flags.at("resettable").get<std::uint32_t>() : 0;
    value |= spec.Secret() ? flags.at("secret").get<std::uint32_t>() : 0;
    value |= spec.ReadOnly() ? flags.at("read_only").get<std::uint32_t>() : 0;
    value |= spec.Critical() ? flags.at("critical").get<std::uint32_t>() : 0;
    return value;
}

// Names the public enums do not know go through RawReset(), as a caller with
// an older build would send them.
template <typename Options, typename Key, std::size_t Count>
void AddReset(Options &options, const Key (&keys)[Count], const std::string &name) {
    for (const auto key : keys) {
        if (iggy::to_string(key) == name) {
            options.Reset(key);
            return;
        }
    }
    options.RawReset(name);
}

template <typename Operation>
::testing::AssertionResult ThrowsIggyError(Operation &&operation, const nlohmann::json &error) {
    const auto code = error.at("code").get<std::uint32_t>();
    const auto kind = error.at("name").get<std::string>();
    try {
        std::forward<Operation>(operation)();
    } catch (const iggy::IggyException &exception) {
        if (exception.Code() == code && exception.Kind() == kind) {
            return ::testing::AssertionSuccess();
        }
        return ::testing::AssertionFailure()
               << "expected " << kind << " (" << code << "), got '" << exception.what() << "' with code "
               << exception.Code().value_or(0) << " and kind '" << exception.Kind() << "'";
    }
    return ::testing::AssertionFailure() << "expected " << kind << " (" << code << "), nothing was thrown";
}

template <typename Details>
void ExpectNoKeyMaterial(const Details &details) {
    for (const auto &key : Fixture().at("constants").at("never_returned_keys")) {
        const auto name = key.get<std::string>();
        EXPECT_EQ(details.Options().Explicit().count(name), 0u) << name;
        EXPECT_EQ(details.Options().Derived().count(name), 0u) << name;
    }
}

template <typename Details>
void ExpectProjection(const Details &details,
                      iggy::StreamEncryptionSource source,
                      const std::optional<std::string> &fingerprint) {
    ASSERT_TRUE(details.EncryptionKeySource().has_value());
    EXPECT_EQ(*details.EncryptionKeySource(), source);
    ASSERT_TRUE(details.EncryptionKeyFingerprint().has_value());
    EXPECT_EQ(details.EncryptionKeyFingerprint()->size(), kFingerprintHex);
    if (fingerprint) {
        EXPECT_EQ(*details.EncryptionKeyFingerprint(), *fingerprint);
    }
    ASSERT_FALSE(details.EncryptionKekFingerprints().empty());
    for (const auto &kek_fingerprint : details.EncryptionKekFingerprints()) {
        EXPECT_EQ(kek_fingerprint.size(), kFingerprintHex);
    }
    EXPECT_EQ(HeaderFieldText(details.Options().Derived().at(
                  std::string(iggy::to_string(iggy::StreamOptionKey::EncryptionKeySource)))),
              iggy::to_string(source));
    ExpectNoKeyMaterial(details);
}

template <typename Left, typename Right>
void ExpectSameProjection(const Left &left, const Right &right) {
    EXPECT_EQ(left.EncryptionKeySource(), right.EncryptionKeySource());
    EXPECT_EQ(left.EncryptionKeyFingerprint(), right.EncryptionKeyFingerprint());
    EXPECT_EQ(left.EncryptionKekFingerprints(), right.EncryptionKekFingerprints());
}

std::optional<iggy::Stream> FindStream(iggy::IggyBlockingClient &client, std::uint32_t stream_id) {
    for (auto &stream : client.GetStreams()) {
        if (stream.Id() == stream_id) {
            return stream;
        }
    }
    return std::nullopt;
}

std::string PrintTransport(const ::testing::TestParamInfo<ResourceOptionsTransport> &info) {
    return info.param.name;
}

}  // namespace

class E2E_ResourceOptions : public E2ETestFixture {};

class E2E_ResourceOptionsTransport : public E2ETestFixture,
                                     public ::testing::WithParamInterface<ResourceOptionsTransport> {
  protected:
    iggy::IggyBlockingClient GetTransportClient() { return GetTransportClient("iggy", "iggy"); }

    iggy::IggyBlockingClient GetTransportClient(const std::string &username, const std::string &password) {
        const auto &transport = GetParam();
        auto client =
            transport.scheme.empty()
                ? iggy::IggyBlockingClient::Builder().WithAutoLogin(username, password).Build()
                : iggy::IggyBlockingClient::FromConnectionString(transport.ConnectionString(username, password));
        client.Connect();
        if (transport.name == "http") {
            client.Login(username, password);
        }
        return client;
    }

    void SendPayload(std::uint32_t stream_id, std::uint32_t topic_id, const std::string &payload) {
        iggy::ffi::Client *client = GetLoggedInClient();
        ASSERT_NE(client, nullptr);
        rust::Vec<iggy::ffi::IggyMessageToSend> messages;
        messages.push_back(iggy::ffi::make_message(to_payload(payload), rust::Vec<iggy::ffi::HeaderEntry>()));
        iggy::ffi::ErrorInfo error_info{};
        ASSERT_NO_THROW(client->send_messages(make_numeric_identifier(stream_id), make_numeric_identifier(topic_id),
                                              "partition_id", partition_id_bytes(0), std::move(messages), error_info));
    }

    std::vector<std::string> PollPayloads(std::uint32_t stream_id, std::uint32_t topic_id) {
        iggy::ffi::Client *client = GetLoggedInClient();
        std::vector<std::string> payloads;
        if (client == nullptr) {
            return payloads;
        }
        iggy::ffi::ErrorInfo error_info{};
        const auto polled =
            client->poll_messages(make_numeric_identifier(stream_id), make_numeric_identifier(topic_id), 0, "consumer",
                                  make_numeric_identifier(1), "offset", 0, 10, false, error_info);
        for (const auto &message : polled.messages) {
            payloads.emplace_back(message.payload.begin(), message.payload.end());
        }
        return payloads;
    }
};

INSTANTIATE_TEST_SUITE_P(Transports,
                         E2E_ResourceOptionsTransport,
                         ::testing::Values(ResourceOptionsTransport{"tcp", "", ""},
                                           ResourceOptionsTransport{"quic", "iggy+quic", "127.0.0.1:8080"},
                                           ResourceOptionsTransport{"http", "iggy+http", "127.0.0.1:3000"},
                                           ResourceOptionsTransport{"websocket", "iggy+ws", "127.0.0.1:8092"}),
                         PrintTransport);

TEST_P(E2E_ResourceOptionsTransport, GivenStreamKeyWhenCreatingShouldReadMatchingProjection) {
    RecordProperty("description",
                   "Creates a stream with its own key and reads source, key fingerprint and KEK fingerprints through "
                   "create, get and list, with no key material in any options block.");
    const auto &stream_key = Fixture().at("stream_key");
    const auto fingerprint = stream_key.at("fingerprint").get<std::string>();
    auto client            = GetTransportClient();
    const auto name        = GetRandomName(40);

    const auto created =
        client.CreateStream(name, iggy::StreamCreateOptions().SetEncryptionKey(FromHex(stream_key.at("key_hex"))));
    TrackStream(name);
    ExpectProjection(created, iggy::StreamEncryptionSource::Stream, fingerprint);

    const auto fetched = client.GetStream(iggy::Identifier::Numeric(created.Id()));
    ExpectProjection(fetched, iggy::StreamEncryptionSource::Stream, fingerprint);
    ExpectSameProjection(created, fetched);

    const auto listed = FindStream(client, created.Id());
    ASSERT_TRUE(listed.has_value());
    ExpectProjection(*listed, iggy::StreamEncryptionSource::Stream, fingerprint);
    ExpectSameProjection(created, *listed);

    EXPECT_EQ(iggy::EncryptionKeyFingerprint(FixtureKey()), fingerprint);
}

TEST_P(E2E_ResourceOptionsTransport, GivenKeylessStreamWhenReadingShouldReportServerGeneratedKey) {
    RecordProperty("description",
                   "Creates a stream without a key on an encryption-enabled server and reads the server-generated "
                   "key's source and fingerprints through the typed accessors.");
    auto client     = GetTransportClient();
    const auto name = GetRandomName(40);

    const auto created = client.CreateStream(name);
    TrackStream(name);
    ExpectProjection(created, iggy::StreamEncryptionSource::Server, std::nullopt);

    const auto fetched = client.GetStream(iggy::Identifier::String(name));
    ExpectProjection(fetched, iggy::StreamEncryptionSource::Server, std::nullopt);
    ExpectSameProjection(created, fetched);

    const auto listed = FindStream(client, created.Id());
    ASSERT_TRUE(listed.has_value());
    ExpectSameProjection(created, *listed);
}

TEST_P(E2E_ResourceOptionsTransport, GivenStreamRenameWhenUpdatingShouldPreserveEncryptionProjection) {
    RecordProperty("description",
                   "Renames a keyed stream through the generic update and keeps its id, source, fingerprints and "
                   "readable payloads.");
    const auto &stream_key = Fixture().at("stream_key");
    auto client            = GetTransportClient();
    const auto name        = GetRandomName(40);
    const auto renamed     = GetRandomName(40);

    const auto created = client.CreateStream(
        name, iggy::StreamCreateOptions().SetEncryptionKeyBase64(stream_key.at("key_base64").get<std::string>()));
    TrackStream(created.Id());
    const auto topic =
        client.CreateTopic(iggy::Identifier::Numeric(created.Id()), "events", iggy::TopicCreateOptions());
    SendPayload(created.Id(), topic.Id(), "before-rename");

    ASSERT_NO_THROW(
        client.UpdateStream(iggy::Identifier::Numeric(created.Id()), iggy::StreamUpdateOptions().SetName(renamed)));

    const auto fetched = client.GetStream(iggy::Identifier::Numeric(created.Id()));
    EXPECT_EQ(fetched.Id(), created.Id());
    EXPECT_EQ(fetched.Name(), renamed);
    ExpectProjection(fetched, iggy::StreamEncryptionSource::Stream, stream_key.at("fingerprint").get<std::string>());
    ExpectSameProjection(created, fetched);
    EXPECT_EQ(PollPayloads(created.Id(), topic.Id()), std::vector<std::string>{"before-rename"});
}

TEST_P(E2E_ResourceOptionsTransport, GivenFixedResizePolicyWhenCreatingTopicShouldStoreItAndRefuseResize) {
    RecordProperty("description",
                   "Creates a topic with the typed fixed resize policy, reads it back as an explicit option and "
                   "refuses a later partition creation.");
    auto client            = GetTransportClient();
    const auto stream_name = GetRandomName(40);
    const auto stream      = client.CreateStream(stream_name);
    TrackStream(stream_name);
    const auto created =
        client.CreateTopic(iggy::Identifier::Numeric(stream.Id()), "fixed",
                           iggy::TopicCreateOptions().SetPartitionResizePolicy(iggy::PartitionResizePolicy::Fixed));

    const auto key = std::string(iggy::to_string(iggy::TopicOptionKey::PartitionResizePolicy));
    ASSERT_EQ(created.Options().Explicit().count(key), 1u);
    EXPECT_EQ(HeaderFieldText(created.Options().Explicit().at(key)), "fixed");
    EXPECT_THROW(
        client.CreatePartitions(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(created.Id()), 1),
        iggy::IggyException);
}

TEST_P(E2E_ResourceOptionsTransport, GivenTopicResetWhenUpdatingShouldPreserveUnmentionedValues) {
    RecordProperty("description",
                   "Sets a topic's expiry without a name and resets its max size, keeping the name, compression and "
                   "the provenance of every other key.");
    constexpr std::uint64_t initial_expiry = 3600000000;
    constexpr std::uint64_t updated_expiry = 7200000000;
    constexpr std::uint64_t max_size       = 1073741824;
    auto client                            = GetTransportClient();
    const auto stream_name                 = GetRandomName(40);
    const auto stream                      = client.CreateStream(stream_name);
    TrackStream(stream_name);
    const auto created = client.CreateTopic(iggy::Identifier::Numeric(stream.Id()), "patched",
                                            iggy::TopicCreateOptions()
                                                .SetCompressionAlgorithm(iggy::CompressionAlgorithm::Gzip())
                                                .SetMessageExpiry(iggy::Expiry::Duration(initial_expiry))
                                                .SetMaxTopicSize(iggy::MaxTopicSize::FromBytes(max_size)));

    ASSERT_NO_THROW(client.UpdateTopic(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(created.Id()),
                                       iggy::TopicUpdateOptions()
                                           .SetMessageExpiry(iggy::Expiry::Duration(updated_expiry))
                                           .Reset(iggy::TopicOptionKey::MaxTopicSize)));

    const auto topic = client.GetTopic(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(created.Id()));
    const auto &options = topic.Options();
    EXPECT_EQ(topic.Name(), "patched");
    EXPECT_EQ(topic.MessageExpiry(), updated_expiry);
    EXPECT_EQ(topic.MaxTopicSize(), std::numeric_limits<std::uint64_t>::max());
    EXPECT_EQ(topic.CompressionAlgorithm(), "gzip");
    EXPECT_EQ(options.Explicit().count("message_expiry"), 1u);
    EXPECT_EQ(options.Explicit().count("max_topic_size"), 0u);
    EXPECT_EQ(options.Derived().count("max_topic_size"), 1u);
    EXPECT_EQ(options.Explicit().count("compression_algorithm"), 1u);
    for (const auto &[key, value] : created.Options().Derived()) {
        if (key != "max_topic_size") {
            ASSERT_EQ(options.Derived().count(key), 1u) << key;
            EXPECT_EQ(options.Derived().at(key).Value(), value.Value()) << key;
        }
    }
}

TEST_P(E2E_ResourceOptionsTransport, GivenPatchAdmissionRefusesWhenUpdatingTopicShouldKeepTopicUnchanged) {
    RecordProperty("description",
                   "Refuses a topic update of only zero sentinels as invalid_command and one with more resets than "
                   "the count byte carries as unsupported_option_key, leaving the topic unchanged.");
    auto client            = GetTransportClient();
    const auto stream_name = GetRandomName(40);
    const auto stream      = client.CreateStream(stream_name);
    TrackStream(stream_name);
    const auto stream_id = iggy::Identifier::Numeric(stream.Id());
    const auto created   = client.CreateTopic(stream_id, "bounded",
                                              iggy::TopicCreateOptions()
                                                  .SetMessageExpiry(iggy::Expiry::Duration(3600000000))
                                                  .SetMaxTopicSize(iggy::MaxTopicSize::FromBytes(1073741824)));
    const auto topic_id  = iggy::Identifier::Numeric(created.Id());

    const auto sentinel_error = FixtureCase("http_update", "topic_zero_sentinel_only").at("error");
    EXPECT_TRUE(ThrowsIggyError(
        [&] {
            client.UpdateTopic(stream_id, topic_id,
                               iggy::TopicUpdateOptions().SetMessageExpiry(iggy::Expiry::ServerDefault()));
        },
        sentinel_error));
    EXPECT_TRUE(ThrowsIggyError(
        [&] {
            client.UpdateTopic(stream_id, topic_id,
                               iggy::TopicUpdateOptions()
                                   .SetMessageExpiry(iggy::Expiry::ServerDefault())
                                   .SetMaxTopicSize(iggy::MaxTopicSize::ServerDefault()));
        },
        sentinel_error));

    iggy::TopicUpdateOptions too_many_resets;
    too_many_resets.Reset(iggy::TopicOptionKey::MaxTopicSize);
    for (std::size_t index = 0; index < kMaxResetKeys; ++index) {
        too_many_resets.RawReset("future_" + std::to_string(index));
    }
    EXPECT_TRUE(ThrowsIggyError([&] { client.UpdateTopic(stream_id, topic_id, too_many_resets); },
                                nlohmann::json{{"name", "unsupported_option_key"}, {"code", 4041}}));

    const auto topic = client.GetTopic(stream_id, topic_id);
    EXPECT_EQ(topic.Name(), "bounded");
    EXPECT_EQ(topic.MessageExpiry(), created.MessageExpiry());
    EXPECT_EQ(topic.MaxTopicSize(), created.MaxTopicSize());
    EXPECT_EQ(topic.Options().Explicit().count("max_topic_size"), 1u);
}

TEST_P(E2E_ResourceOptionsTransport, GivenUserOptionsWhenCreatingAndUpdatingShouldPreserveUserBehavior) {
    RecordProperty("description",
                   "Creates a user with typed empty options, patches its username and status, reads its options with "
                   "provenance and refuses a duplicate username without a partial change.");
    auto client               = GetTransportClient();
    const auto username       = GetRandomName(30);
    const auto renamed        = GetRandomName(30);
    const auto other_username = GetRandomName(30);
    const auto created =
        CreateUser(client, username, "user-secret", iggy::UserStatus::Active, std::nullopt, iggy::UserCreateOptions());
    CreateUser(client, other_username, "user-secret", iggy::UserStatus::Active);
    EXPECT_TRUE(created.Options().Explicit().empty());

    ASSERT_NO_THROW(
        client.UpdateUser(iggy::Identifier::Numeric(created.Id()),
                          iggy::UserUpdateOptions().SetUsername(renamed).SetStatus(iggy::UserStatus::Inactive)));
    RenameTrackedUser(username, renamed);
    const auto updated = client.GetUser(iggy::Identifier::Numeric(created.Id()));
    EXPECT_EQ(updated.Username(), renamed);
    EXPECT_EQ(updated.Status(), iggy::UserStatus::Inactive);
    EXPECT_EQ(updated.CreatedAt(), created.CreatedAt());
    EXPECT_TRUE(updated.Options().Explicit().empty());

    EXPECT_THROW(
        client.UpdateUser(iggy::Identifier::String(other_username),
                          iggy::UserUpdateOptions().SetUsername(renamed).SetStatus(iggy::UserStatus::Inactive)),
        iggy::IggyException);
    const auto other = client.GetUser(iggy::Identifier::String(other_username));
    EXPECT_EQ(other.Username(), other_username);
    EXPECT_EQ(other.Status(), iggy::UserStatus::Active);
}

TEST_F(E2E_ResourceOptions, GivenInvalidKeyWhenEncodingShouldRejectBeforeNetworkIo) {
    RecordProperty("description",
                   "Refuses every invalid key vector with invalid_encryption_key on a client that never connected, "
                   "accepts exactly 32 bytes and keeps key material out of the error text.");
    const auto &stream_key = Fixture().at("stream_key");
    const auto key_hex     = stream_key.at("key_hex").get<std::string>();
    const auto key_base64  = stream_key.at("key_base64").get<std::string>();
    auto client            = GetLoggedOutHighLevelClient();

    for (const auto &vector : stream_key.at("invalid_keys")) {
        SCOPED_TRACE(vector.at("case").get<std::string>());
        const auto &input = vector.at("input");
        iggy::StreamCreateOptions options;
        if (input.contains("raw_hex")) {
            const auto raw = FromHex(input.at("raw_hex").get<std::string>());
            options.SetEncryptionKey(raw);
            EXPECT_TRUE(ThrowsIggyError([&raw] { iggy::EncryptionKeyFingerprint(raw); }, vector.at("error")));
        } else {
            options.SetEncryptionKeyBase64(input.at("base64").get<std::string>());
        }
        EXPECT_TRUE(
            ThrowsIggyError([&client, &options] { client.CreateStream("never-sent", options); }, vector.at("error")));
        try {
            client.CreateStream("never-sent", options);
        } catch (const iggy::IggyException &error) {
            const std::string message = error.what();
            EXPECT_EQ(message.find(key_base64.substr(0, 40)), std::string::npos);
            EXPECT_EQ(message.find(key_hex.substr(0, 40)), std::string::npos);
        }
    }

    EXPECT_EQ(iggy::EncryptionKeyFingerprint(FixtureKey()), stream_key.at("fingerprint").get<std::string>());
    try {
        client.CreateStream("never-sent", iggy::StreamCreateOptions().SetEncryptionKeyBase64(key_base64));
        FAIL() << "A client that never connected cannot create a stream";
    } catch (const iggy::IggyException &error) {
        EXPECT_NE(error.Kind(), "invalid_encryption_key");
    }
}

TEST_P(E2E_ResourceOptionsTransport, GivenSharedVectorsWhenEncodingShouldMatchServerContract) {
    RecordProperty("description",
                   "Matches the shared fixture on every transport: update refusals in the server order, raw resets "
                   "decided by the server and stream create admission.");
    const auto &fixture = Fixture();
    EXPECT_EQ(fixture.at("schema_version").get<int>(), 1);

    auto client               = GetTransportClient();
    auto root                 = GetLoggedInHighLevelClient();
    const auto raw_reset_user = CreateUser(root, GetRandomName(30), "raw-reset-secret", iggy::UserStatus::Active).Id();
    for (const auto &vector : fixture.at("update_resource_errors")) {
        const auto scope = vector.at("scope").get<std::string>();
        SCOPED_TRACE(vector.at("case").get<std::string>());
        const auto &target = vector.at("target");
        const auto &input  = vector.at("input");
        if (scope == "topic") {
            iggy::TopicUpdateOptions options;
            for (const auto &[key, value] : input.items()) {
                if (key == "message_expiry") {
                    options.SetMessageExpiry(iggy::Expiry::Duration(std::stoull(value.get<std::string>())));
                } else if (key == "max_topic_size") {
                    options.SetMaxTopicSize(iggy::MaxTopicSize::FromBytes(std::stoull(value.get<std::string>())));
                } else if (key == "name") {
                    options.SetName(value.get<std::string>());
                } else if (key == "reset") {
                    for (const auto &reset : value) {
                        AddReset(options, kTopicOptionKeys, reset.get<std::string>());
                    }
                } else {
                    FAIL() << "Unmapped topic input " << key;
                }
            }
            EXPECT_TRUE(ThrowsIggyError(
                [&] {
                    client.UpdateTopic(iggy::Identifier::Numeric(target.at("stream_id").get<std::uint32_t>()),
                                       iggy::Identifier::Numeric(target.at("topic_id").get<std::uint32_t>()), options);
                },
                vector.at("error")));
        } else if (scope == "stream") {
            iggy::StreamUpdateOptions options;
            if (input.contains("name")) {
                options.SetName(input.at("name").get<std::string>());
            }
            for (const auto &reset : input.value("reset", nlohmann::json::array())) {
                AddReset(options, kStreamOptionKeys, reset.get<std::string>());
            }
            EXPECT_TRUE(ThrowsIggyError(
                [&] {
                    client.UpdateStream(iggy::Identifier::Numeric(target.at("stream_id").get<std::uint32_t>()),
                                        options);
                },
                vector.at("error")));
        } else {
            iggy::UserUpdateOptions options;
            if (input.contains("username")) {
                options.SetUsername(input.at("username").get<std::string>());
            }
            for (const auto &reset : input.value("reset", nlohmann::json::array())) {
                AddReset(options, kUserOptionKeys, reset.get<std::string>());
            }
            // A raw reset reaches the server, so it needs a user that exists.
            const auto user_id =
                options.RawResetKeys().empty() ? target.at("user_id").get<std::uint32_t>() : raw_reset_user;
            EXPECT_TRUE(ThrowsIggyError([&] { client.UpdateUser(iggy::Identifier::Numeric(user_id), options); },
                                        vector.at("error")));
        }
    }

    for (const auto &vector : fixture.at("create_stream")) {
        SCOPED_TRACE(vector.at("case").get<std::string>());
        const auto &input = vector.at("input");
        const auto name   = GetRandomName(40);
        iggy::StreamCreateOptions options;
        if (input.contains("encryption_key")) {
            options.SetEncryptionKey(FromHex(input.at("encryption_key").at("raw_hex").get<std::string>()));
        }
        if (input.contains("raw")) {
            options.SetRawEntries(input.at("raw").get<std::map<std::string, std::string>>());
        }
        if (vector.at("admission_error").is_null()) {
            const auto created = client.CreateStream(name, options);
            TrackStream(name);
            const auto source = input.contains("encryption_key") ? iggy::StreamEncryptionSource::Stream
                                                                 : iggy::StreamEncryptionSource::Server;
            ExpectProjection(
                created, source,
                input.contains("encryption_key")
                    ? std::optional<std::string>(fixture.at("stream_key").at("fingerprint").get<std::string>())
                    : std::nullopt);
        } else {
            EXPECT_TRUE(ThrowsIggyError([&] { client.CreateStream(name, options); }, vector.at("admission_error")));
            EXPECT_THROW(client.GetStream(iggy::Identifier::String(name)), iggy::IggyException);
        }
    }
}

TEST_P(E2E_ResourceOptionsTransport, GivenLoggedInUserRenameWhenReconnectingShouldKeepNewUsername) {
    RecordProperty("description",
                   "Renames the signed-in user, reconnects with the new name, keeps it after a refused rename and is "
                   "not affected by a stream rename.");
    if (GetParam().name == "http") {
        GTEST_SKIP() << "HTTP is stateless and does not reconnect";
    }
    auto root                = GetLoggedInHighLevelClient();
    const auto username      = GetRandomName(30);
    const auto renamed       = GetRandomName(30);
    const auto taken         = GetRandomName(30);
    const std::string secret = "rename-secret";
    iggy::GlobalPermissions global;
    global.SetManageUsers(true).SetManageStreams(true);
    iggy::Permissions permissions;
    permissions.SetGlobal(global);
    CreateUser(root, username, secret, iggy::UserStatus::Active, permissions);
    CreateUser(root, taken, secret, iggy::UserStatus::Active);

    auto client = GetTransportClient(username, secret);

    ASSERT_NO_THROW(
        client.UpdateUser(iggy::Identifier::String(username), iggy::UserUpdateOptions().SetUsername(renamed)));
    RenameTrackedUser(username, renamed);
    ASSERT_NO_THROW(client.Disconnect());
    ASSERT_NO_THROW(client.Connect());
    ASSERT_NO_THROW(client.GetMe());

    EXPECT_THROW(client.UpdateUser(iggy::Identifier::String(renamed), iggy::UserUpdateOptions().SetUsername(taken)),
                 iggy::IggyException);
    ASSERT_NO_THROW(client.Disconnect());
    ASSERT_NO_THROW(client.Connect());
    ASSERT_NO_THROW(client.GetMe());

    const auto stream_name = GetRandomName(40);
    const auto stream      = client.CreateStream(stream_name);
    TrackStream(stream.Id());
    ASSERT_NO_THROW(
        client.UpdateStream(iggy::Identifier::Numeric(stream.Id()), iggy::StreamUpdateOptions().SetName(username)));
    ASSERT_NO_THROW(client.Disconnect());
    ASSERT_NO_THROW(client.Connect());
    EXPECT_EQ(root.GetUser(iggy::Identifier::String(renamed)).Username(), renamed);
    ASSERT_NO_THROW(client.GetMe());
}

TEST_P(E2E_ResourceOptionsTransport, GivenPublicDescribeOptionsWhenCalledShouldReturnCatalogFlags) {
    RecordProperty("description",
                   "Serves every scope's catalog through the public DescribeOptions on every transport with the "
                   "fixture's keys, kinds, flags and defaults.");
    const auto &fixture = Fixture();
    std::map<std::string, std::uint8_t> kind_codes;
    for (const auto &row : fixture.at("catalog")) {
        kind_codes[row.at("kind").get<std::string>()] = row.at("kind_code").get<std::uint8_t>();
    }
    const std::map<std::string, iggy::OptionsScope> scopes = {
        {"topic", iggy::OptionsScope::Topic},
        {"stream", iggy::OptionsScope::Stream},
        {"user", iggy::OptionsScope::User},
    };
    auto client = GetTransportClient();

    for (const auto &described : fixture.at("describe_options")) {
        const auto scope = described.at("scope").get<std::string>();
        SCOPED_TRACE(scope);
        const auto specs     = client.DescribeOptions(scopes.at(scope));
        const auto &expected = described.at("entries");
        ASSERT_EQ(specs.size(), expected.size());
        for (std::size_t index = 0; index < specs.size(); ++index) {
            const auto &spec  = specs[index];
            const auto &entry = expected[index];
            SCOPED_TRACE(spec.Key());
            EXPECT_EQ(spec.Key(), entry.at("key").get<std::string>());
            EXPECT_EQ(static_cast<std::uint8_t>(spec.Kind()), kind_codes.at(entry.at("kind").get<std::string>()));
            EXPECT_EQ(FlagsOf(spec), entry.at("flags").get<std::uint32_t>());
            EXPECT_EQ(DefaultText(spec), entry.at("default").is_null()
                                             ? std::nullopt
                                             : std::optional<std::string>(entry.at("default").get<std::string>()));
            EXPECT_FALSE(spec.Description().empty());
        }
    }

    const auto stream_specs = client.DescribeOptions(iggy::OptionsScope::Stream);
    const auto spec_of      = [&stream_specs](iggy::StreamOptionKey key) {
        for (const auto &spec : stream_specs) {
            if (spec.Key() == iggy::to_string(key)) {
                return spec;
            }
        }
        throw std::runtime_error("Missing stream option");
    };
    const auto name = spec_of(iggy::StreamOptionKey::Name);
    EXPECT_TRUE(name.Updatable() && !name.Secret() && !name.ReadOnly());
    const auto key = spec_of(iggy::StreamOptionKey::EncryptionKey);
    EXPECT_TRUE(!key.Updatable() && !key.Resettable() && key.Secret() && key.Critical() && !key.ReadOnly());
    const auto fingerprint = spec_of(iggy::StreamOptionKey::EncryptionKeyFingerprint);
    EXPECT_TRUE(fingerprint.ReadOnly() && !fingerprint.Updatable() && !fingerprint.Secret());
}
