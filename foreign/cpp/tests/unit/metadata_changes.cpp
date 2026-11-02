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

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <functional>
#include <map>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "iggy.hpp"

namespace {

constexpr char kFixturePath[]           = "tests/fixtures/metadata_changes.json";
constexpr std::uint32_t kInvalidCommand = 3;
constexpr std::uint32_t kInvalidFormat  = 4;

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

std::vector<std::uint8_t> FromHex(const std::string &hex) {
    std::vector<std::uint8_t> bytes;
    bytes.reserve(hex.size() / 2);
    for (std::size_t index = 0; index + 1 < hex.size(); index += 2) {
        bytes.push_back(static_cast<std::uint8_t>(std::stoul(hex.substr(index, 2), nullptr, 16)));
    }
    return bytes;
}

std::string ToHex(const std::vector<std::uint8_t> &bytes) {
    static constexpr char kDigits[] = "0123456789abcdef";
    std::string hex;
    hex.reserve(bytes.size() * 2);
    for (const auto byte : bytes) {
        hex.push_back(kDigits[byte >> 4]);
        hex.push_back(kDigits[byte & 0x0F]);
    }
    return hex;
}

std::string ToHex(absl::uint128 value) {
    std::vector<std::uint8_t> bytes(16);
    for (std::size_t index = 16; index > 0; --index) {
        bytes[index - 1] = static_cast<std::uint8_t>(absl::Uint128Low64(value) & 0xFF);
        value >>= 8;
    }
    return ToHex(bytes);
}

std::string ToBase64(const std::vector<std::uint8_t> &bytes) {
    static constexpr char kAlphabet[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string text;
    text.reserve((bytes.size() + 2) / 3 * 4);
    for (std::size_t index = 0; index < bytes.size(); index += 3) {
        const std::size_t remaining = bytes.size() - index;
        std::uint32_t chunk         = static_cast<std::uint32_t>(bytes[index]) << 16;
        if (remaining > 1) {
            chunk |= static_cast<std::uint32_t>(bytes[index + 1]) << 8;
        }
        if (remaining > 2) {
            chunk |= bytes[index + 2];
        }
        text.push_back(kAlphabet[(chunk >> 18) & 0x3F]);
        text.push_back(kAlphabet[(chunk >> 12) & 0x3F]);
        text.push_back(remaining > 1 ? kAlphabet[(chunk >> 6) & 0x3F] : '=');
        text.push_back(remaining > 2 ? kAlphabet[chunk & 0x3F] : '=');
    }
    return text;
}

std::uint64_t ReadLittleEndian(const std::vector<std::uint8_t> &bytes) {
    std::uint64_t value = 0;
    for (std::size_t index = bytes.size(); index > 0; --index) {
        value = (value << 8) | bytes[index - 1];
    }
    return value;
}

std::string HeaderKindName(iggy::HeaderKind kind) {
    static const char *const kNames[] = {"raw",    "string", "bool",    "int8",    "int16",
                                         "int32",  "int64",  "int128",  "uint8",   "uint16",
                                         "uint32", "uint64", "uint128", "float32", "float64"};
    return kNames[static_cast<std::size_t>(kind) - 1];
}

std::string HeaderValueText(const iggy::HeaderField &field) {
    const auto &bytes = field.Value();
    switch (field.Kind()) {
        case iggy::HeaderKind::String:
            return std::string(bytes.begin(), bytes.end());
        case iggy::HeaderKind::Bool:
            return bytes.front() == 0 ? "false" : "true";
        case iggy::HeaderKind::Uint8:
        case iggy::HeaderKind::Uint16:
        case iggy::HeaderKind::Uint32:
        case iggy::HeaderKind::Uint64:
            return std::to_string(ReadLittleEndian(bytes));
        default:
            throw std::invalid_argument("No text form for option kind " + HeaderKindName(field.Kind()));
    }
}

nlohmann::json OptionsJson(const std::optional<iggy::ResourceOptions> &options) {
    if (!options) {
        return nullptr;
    }
    auto json         = nlohmann::json::object();
    const auto render = [&json](const std::map<std::string, iggy::HeaderField> &entries, bool is_explicit) {
        for (const auto &[key, field] : entries) {
            json[key] = {
                {"value", HeaderValueText(field)}, {"kind", HeaderKindName(field.Kind())}, {"explicit", is_explicit}};
        }
    };
    render(options->Explicit(), true);
    render(options->Derived(), false);
    return json;
}

nlohmann::json PermissionsJson(const std::optional<iggy::Permissions> &permissions) {
    if (!permissions) {
        return nullptr;
    }
    const auto &global = permissions->Global();
    nlohmann::json json{{"global",
                         {{"manage_servers", global.ManageServers()},
                          {"read_servers", global.ReadServers()},
                          {"manage_users", global.ManageUsers()},
                          {"read_users", global.ReadUsers()},
                          {"manage_streams", global.ManageStreams()},
                          {"read_streams", global.ReadStreams()},
                          {"manage_topics", global.ManageTopics()},
                          {"read_topics", global.ReadTopics()},
                          {"poll_messages", global.PollMessages()},
                          {"send_messages", global.SendMessages()}}},
                        {"streams", nullptr}};
    for (const auto &[stream_id, stream] : permissions->Streams()) {
        nlohmann::json stream_json{{"manage_stream", stream.ManageStream()},
                                   {"read_stream", stream.ReadStream()},
                                   {"manage_topics", stream.ManageTopics()},
                                   {"read_topics", stream.ReadTopics()},
                                   {"poll_messages", stream.PollMessages()},
                                   {"send_messages", stream.SendMessages()},
                                   {"topics", nullptr}};
        for (const auto &[topic_id, topic] : stream.Topics()) {
            stream_json["topics"][std::to_string(topic_id)] = {{"manage_topic", topic.ManageTopic()},
                                                               {"read_topic", topic.ReadTopic()},
                                                               {"poll_messages", topic.PollMessages()},
                                                               {"send_messages", topic.SendMessages()}};
        }
        json["streams"][std::to_string(stream_id)] = std::move(stream_json);
    }
    return json;
}

template <typename Body>
nlohmann::json StreamKeyJson(const Body &body) {
    return {{"stream_id", body.StreamId()}, {"stream_created_op", body.StreamCreatedOp()}};
}

template <typename Body>
nlohmann::json TopicKeyJson(const Body &body) {
    auto json                = StreamKeyJson(body);
    json["topic_id"]         = body.TopicId();
    json["topic_created_op"] = body.TopicCreatedOp();
    return json;
}

template <typename Body>
nlohmann::json UserKeyJson(const Body &body) {
    return {{"user_id", body.UserId()}, {"user_created_at", body.UserCreatedAt()}};
}

void SetOptional(nlohmann::json &json, const char *key, const std::optional<std::uint64_t> &value) {
    if (value) {
        json[key] = *value;
    }
}

void SetOptions(nlohmann::json &json, const std::optional<iggy::ResourceOptions> &options) {
    json["options"] = OptionsJson(options);
    if (!options) {
        json["options_unreadable"] = true;
    }
}

// The JSON the HTTP route renders for each body, so every vector compares whole.
struct BodyJson {
    nlohmann::json operator()(const iggy::MetadataStreamChange &body) const {
        auto json          = StreamKeyJson(body);
        json["created_at"] = body.CreatedAt();
        json["name"]       = body.Name();
        SetOptions(json, body.Options());
        return json;
    }

    nlohmann::json operator()(const iggy::MetadataStreamDeleted &body) const {
        auto json          = StreamKeyJson(body);
        json["created_at"] = body.CreatedAt();
        json["name"]       = body.Name();
        SetOptional(json, "requested_op", body.RequestedOp());
        return json;
    }

    nlohmann::json operator()(const iggy::MetadataTopicChange &body) const {
        auto json                = TopicKeyJson(body);
        json["created_at"]       = body.CreatedAt();
        json["name"]             = body.Name();
        json["partitions_count"] = body.PartitionsCount();
        SetOptions(json, body.Options());
        return json;
    }

    nlohmann::json operator()(const iggy::MetadataTopicItem &body) const {
        auto json          = TopicKeyJson(body);
        json["created_at"] = body.CreatedAt();
        json["name"]       = body.Name();
        SetOptions(json, body.Options());
        return json;
    }

    nlohmann::json operator()(const iggy::MetadataTopicDeleted &body) const {
        auto json          = TopicKeyJson(body);
        json["created_at"] = body.CreatedAt();
        json["name"]       = body.Name();
        SetOptional(json, "requested_op", body.RequestedOp());
        return json;
    }

    nlohmann::json operator()(const iggy::MetadataPartitionsChanged &body) const {
        auto json                     = TopicKeyJson(body);
        json["partitions_count"]      = body.PartitionsCount();
        json["removed_partition_ids"] = body.RemovedPartitionIds();
        SetOptional(json, "requested_op", body.RequestedOp());
        return json;
    }

    nlohmann::json operator()(const iggy::MetadataPartitionsItem &body) const {
        auto json                = TopicKeyJson(body);
        json["partitions_count"] = body.PartitionsCount();
        return json;
    }

    nlohmann::json operator()(const iggy::MetadataConsumerGroupCreated &body) const {
        auto json                     = TopicKeyJson(body);
        json["group_id"]              = body.GroupId();
        json["name"]                  = body.Name();
        json["consumer_groups_count"] = body.ConsumerGroupsCount();
        return json;
    }

    nlohmann::json operator()(const iggy::MetadataConsumerGroupDeleted &body) const {
        auto json                     = TopicKeyJson(body);
        json["group_id"]              = body.GroupId();
        json["name"]                  = body.Name();
        json["consumer_groups_count"] = body.ConsumerGroupsCount();
        SetOptional(json, "requested_op", body.RequestedOp());
        return json;
    }

    nlohmann::json operator()(const iggy::MetadataConsumerGroupItem &body) const {
        auto json        = TopicKeyJson(body);
        json["group_id"] = body.GroupId();
        json["name"]     = body.Name();
        return json;
    }

    nlohmann::json operator()(const iggy::MetadataMemberJoined &body) const {
        auto json             = TopicKeyJson(body);
        json["group_id"]      = body.GroupId();
        json["member_id"]     = body.MemberId();
        json["client_id"]     = ToHex(body.ClientId());
        json["members_count"] = body.MembersCount();
        json["partition_ids"] = body.PartitionIds();
        return json;
    }

    nlohmann::json operator()(const iggy::MetadataMemberLeft &body) const {
        auto json             = TopicKeyJson(body);
        json["group_id"]      = body.GroupId();
        json["member_id"]     = body.MemberId();
        json["client_id"]     = ToHex(body.ClientId());
        json["members_count"] = body.MembersCount();
        json["reason"]        = iggy::to_string(body.Reason());
        return json;
    }

    nlohmann::json operator()(const iggy::MetadataPartitionHandedOver &body) const {
        auto json            = TopicKeyJson(body);
        json["group_id"]     = body.GroupId();
        json["partition_id"] = body.PartitionId();
        if (body.MemberId()) {
            json["member_id"] = *body.MemberId();
        }
        return json;
    }

    nlohmann::json operator()(const iggy::MetadataMemberItem &body) const {
        auto json             = TopicKeyJson(body);
        json["group_id"]      = body.GroupId();
        json["member_id"]     = body.MemberId();
        json["client_id"]     = ToHex(body.ClientId());
        json["partition_ids"] = body.PartitionIds();
        return json;
    }

    nlohmann::json operator()(const iggy::MetadataUserChange &body) const {
        auto json           = UserKeyJson(body);
        json["username"]    = body.Username();
        json["status"]      = body.Status() == iggy::UserStatus::Active ? "active" : "inactive";
        json["permissions"] = PermissionsJson(body.Permissions());
        SetOptions(json, body.Options());
        return json;
    }

    nlohmann::json operator()(const iggy::MetadataUserIdentity &body) const {
        auto json        = UserKeyJson(body);
        json["username"] = body.Username();
        return json;
    }

    nlohmann::json operator()(const iggy::MetadataUserPermissionsUpdated &body) const {
        auto json           = UserKeyJson(body);
        json["username"]    = body.Username();
        json["permissions"] = PermissionsJson(body.Permissions());
        json["reason"]      = iggy::to_string(body.Reason());
        return json;
    }

    nlohmann::json operator()(const iggy::MetadataTokenChange &body) const {
        auto json    = UserKeyJson(body);
        json["name"] = body.Name();
        SetOptional(json, "expiry", body.Expiry());
        return json;
    }

    nlohmann::json operator()(const iggy::MetadataTokenDeleted &body) const {
        auto json      = UserKeyJson(body);
        json["name"]   = body.Name();
        json["reason"] = iggy::to_string(body.Reason());
        return json;
    }

    nlohmann::json operator()(const iggy::MetadataSession &body) const {
        auto json         = UserKeyJson(body);
        json["client_id"] = ToHex(body.ClientId());
        json["session"]   = body.Session();
        return json;
    }

    nlohmann::json operator()(const iggy::MetadataSessionClosed &body) const {
        auto json         = UserKeyJson(body);
        json["client_id"] = ToHex(body.ClientId());
        json["session"]   = body.Session();
        json["reason"]    = iggy::to_string(body.Reason());
        return json;
    }

    nlohmann::json operator()(const iggy::MetadataUnknownChange &body) const {
        return {{"group", std::to_string(body.GroupCode())},
                {"kind", std::to_string(body.KindCode())},
                {"body", ToBase64(body.Body())}};
    }

    nlohmann::json operator()(const iggy::MetadataUnknownJsonChange &body) const {
        return nlohmann::json::parse(body.Json());
    }

    nlohmann::json operator()(const iggy::MetadataUnknownItem &body) const {
        return {{"group", std::to_string(body.GroupCode())}, {"body", ToBase64(body.Body())}};
    }

    nlohmann::json operator()(const iggy::MetadataUnknownJsonItem &body) const {
        return nlohmann::json::parse(body.Json());
    }
};

nlohmann::json RecordJson(const iggy::MetadataChangeRecord &record) {
    nlohmann::json json{{"op", record.Op()},
                        {"ordinal", record.Ordinal()},
                        {"checksum", ToHex(record.Checksum())},
                        {"timestamp", record.Timestamp()}};
    if (record.ActorUserId()) {
        json["actor_user_id"] = *record.ActorUserId();
    }
    if (record.Kind()) {
        json["group"] = iggy::to_string(*record.Group());
        json["kind"]  = iggy::to_string(*record.Kind());
    }
    json.update(std::visit(BodyJson{}, record.Change()));
    return json;
}

nlohmann::json ItemJson(const iggy::MetadataCurrentItem &item) {
    auto json = std::visit(BodyJson{}, item.Value());
    if (item.Group()) {
        json["group"] = iggy::to_string(*item.Group());
    }
    return json;
}

nlohmann::json AnswerJson(const iggy::MetadataChanges &answer) {
    nlohmann::json json{{"status", iggy::to_string(answer.Status())},
                        {"phase", iggy::to_string(answer.Phase())},
                        {"log_id", nullptr},
                        {"oldest_op", answer.OldestOp()},
                        {"retained_since", answer.RetainedSince()},
                        {"head_op", answer.HeadOp()},
                        {"next", nullptr},
                        {"has_more", answer.HasMore()},
                        {"records", nlohmann::json::array()},
                        {"items", nlohmann::json::array()},
                        {"current_page_token", nullptr}};
    if (answer.LogId()) {
        json["log_id"] = ToHex(*answer.LogId());
    }
    if (answer.Next()) {
        json["next"] = answer.Next()->ToString();
    }
    for (const auto &record : answer.Records()) {
        json["records"].push_back(RecordJson(record));
    }
    for (const auto &item : answer.Items()) {
        json["items"].push_back(ItemJson(item));
    }
    if (answer.CurrentPageToken()) {
        json["current_page_token"] = answer.CurrentPageToken()->ToString();
    }
    return json;
}

// Walks the codes until to_string refuses one, so a value added to a public
// enum is compared with the fixture without a second list here.
template <typename Code>
std::map<std::string, std::uint32_t> PublicCodes() {
    std::map<std::string, std::uint32_t> codes;
    for (unsigned value = 0; value <= 255; ++value) {
        try {
            codes.emplace(iggy::to_string(static_cast<Code>(value)), value);
        } catch (const std::invalid_argument &) {
        }
    }
    return codes;
}

std::map<std::string, std::uint32_t> FixtureCodes(const nlohmann::json &table) {
    std::map<std::string, std::uint32_t> codes;
    for (const auto &[name, value] : table.items()) {
        codes.emplace(name, value.is_object() ? value.at("code").get<std::uint32_t>() : value.get<std::uint32_t>());
    }
    return codes;
}

iggy::MetadataChangeGroup GroupNamed(const std::string &name) {
    for (const auto &[group_name, code] : PublicCodes<iggy::MetadataChangeGroup>()) {
        if (group_name == name) {
            return static_cast<iggy::MetadataChangeGroup>(code);
        }
    }
    throw std::invalid_argument("Unknown group " + name);
}

iggy::MetadataChangesQuery QueryFrom(const nlohmann::json &input) {
    const auto start = input.at("start").get<std::string>();
    auto query       = iggy::MetadataChangesQuery::Earliest();
    if (start == "latest") {
        query = iggy::MetadataChangesQuery::Latest();
    } else if (start == "after") {
        query = iggy::MetadataChangesQuery::After(iggy::MetadataChangeCursor::Parse(input.at("cursor")));
    } else if (start == "current") {
        query =
            input.at("page_token").is_null()
                ? iggy::MetadataChangesQuery::Current()
                : iggy::MetadataChangesQuery::Current(iggy::MetadataCurrentPageToken::Parse(input.at("page_token")));
    }
    std::vector<iggy::MetadataChangeGroup> groups;
    for (const auto &name : input.at("groups")) {
        groups.push_back(GroupNamed(name));
    }
    query.SetGroups(std::move(groups)).SetLimit(input.at("limit").get<std::uint32_t>());
    if (!input.at("since").is_null()) {
        query.SetSince(input.at("since").get<std::uint64_t>());
    }
    if (!input.at("until").is_null()) {
        query.SetUntil(input.at("until").get<std::uint64_t>());
    }
    return query;
}

// A record envelope as the fixture's record_hex convention lays it out, with no actor.
std::vector<std::uint8_t> RecordEnvelope(std::uint8_t group, std::uint8_t kind, const std::vector<std::uint8_t> &body) {
    std::vector<std::uint8_t> bytes(8 + 4 + 16 + 8 + 1, 0);
    bytes.push_back(group);
    bytes.push_back(kind);
    const auto length = static_cast<std::uint32_t>(body.size());
    for (unsigned shift = 0; shift < 32; shift += 8) {
        bytes.push_back(static_cast<std::uint8_t>((length >> shift) & 0xFF));
    }
    bytes.insert(bytes.end(), body.begin(), body.end());
    return bytes;
}

::testing::AssertionResult ThrowsIggyError(const std::function<void()> &operation,
                                           std::uint32_t code,
                                           const std::string &kind) {
    try {
        operation();
    } catch (const iggy::IggyException &exception) {
        if (exception.Code() == code && exception.Kind() == kind) {
            return ::testing::AssertionSuccess();
        }
        return ::testing::AssertionFailure() << "expected " << kind << " (" << code << "), got '" << exception.what()
                                             << "' with kind '" << exception.Kind() << "'";
    }
    return ::testing::AssertionFailure() << "expected " << kind << " (" << code << "), nothing was thrown";
}

}  // namespace

TEST(MetadataChangesFixtureTest, GivenFixtureConstantsWhenComparingPublicEnumsShouldMatchCodesAndNames) {
    const auto &constants = Fixture().at("constants");
    EXPECT_EQ(PublicCodes<iggy::MetadataChangeGroup>(), FixtureCodes(constants.at("groups")));
    EXPECT_EQ(PublicCodes<iggy::MetadataChangeKind>(), FixtureCodes(constants.at("kinds")));
    EXPECT_EQ(PublicCodes<iggy::MetadataChangeStatus>(), FixtureCodes(constants.at("statuses")));
    EXPECT_EQ(PublicCodes<iggy::MetadataChangePhase>(), FixtureCodes(constants.at("phases")));
    const auto &start_modes = constants.at("start_modes");
    EXPECT_EQ(static_cast<std::uint32_t>(iggy::MetadataChangesQuery::Start::Earliest), start_modes.at("earliest"));
    EXPECT_EQ(static_cast<std::uint32_t>(iggy::MetadataChangesQuery::Start::Latest), start_modes.at("latest"));
    EXPECT_EQ(static_cast<std::uint32_t>(iggy::MetadataChangesQuery::Start::After), start_modes.at("after"));
    EXPECT_EQ(static_cast<std::uint32_t>(iggy::MetadataChangesQuery::Start::Current), start_modes.at("current"));
    EXPECT_EQ(PublicCodes<iggy::MetadataMemberLeftReason>(), FixtureCodes(constants.at("member_left_reasons")));
    EXPECT_EQ(PublicCodes<iggy::MetadataTokenDeletedReason>(), FixtureCodes(constants.at("token_deleted_reasons")));
    EXPECT_EQ(PublicCodes<iggy::MetadataSessionClosedReason>(), FixtureCodes(constants.at("session_closed_reasons")));
    EXPECT_EQ(PublicCodes<iggy::MetadataPermissionsUpdatedReason>(),
              FixtureCodes(constants.at("permissions_updated_reasons")));
    EXPECT_EQ(iggy::MetadataChangesQuery::kDefaultLimit, constants.at("default_limit").get<std::uint32_t>());
    EXPECT_EQ(iggy::MetadataChangesQuery::kMaxLimit, constants.at("max_limit").get<std::uint32_t>());
    EXPECT_EQ(iggy::MetadataChangesQuery::Earliest().Limit(), iggy::MetadataChangesQuery::kDefaultLimit);
}

TEST(MetadataChangesFixtureTest, GivenRecordVectorsWhenDecodingShouldReachTheExpectedJsonForEveryKind) {
    std::set<std::uint32_t> kinds;
    for (const auto &vector : Fixture().at("records")) {
        SCOPED_TRACE(vector.at("case").get<std::string>());
        const auto record = iggy::MetadataChangeRecord::FromBytes(FromHex(vector.at("record_hex")));
        ASSERT_TRUE(record.Kind().has_value());
        EXPECT_EQ(static_cast<std::uint32_t>(*record.Kind()), vector.at("kind").get<std::uint32_t>());
        EXPECT_EQ(static_cast<std::uint32_t>(*record.Group()), vector.at("group").get<std::uint32_t>());
        EXPECT_EQ(RecordJson(record), vector.at("expected"));
        kinds.insert(static_cast<std::uint32_t>(*record.Kind()));
    }
    std::set<std::uint32_t> catalog;
    for (const auto &[name, code] : PublicCodes<iggy::MetadataChangeKind>()) {
        catalog.insert(code);
    }
    EXPECT_EQ(kinds, catalog);
}

TEST(MetadataChangesFixtureTest, GivenItemVectorsWhenDecodingShouldReachTheExpectedJsonForEveryGroup) {
    std::set<std::uint32_t> groups;
    for (const auto &vector : Fixture().at("items")) {
        SCOPED_TRACE(vector.at("case").get<std::string>());
        const auto item = iggy::MetadataCurrentItem::FromBytes(FromHex(vector.at("item_hex")));
        ASSERT_TRUE(item.Group().has_value());
        EXPECT_EQ(static_cast<std::uint32_t>(*item.Group()), vector.at("group").get<std::uint32_t>());
        EXPECT_EQ(ItemJson(item), vector.at("expected"));
        groups.insert(static_cast<std::uint32_t>(*item.Group()));
    }
    EXPECT_EQ(groups.size(), PublicCodes<iggy::MetadataChangeGroup>().size());
}

TEST(MetadataChangesFixtureTest, GivenResponseVectorsWhenDecodingShouldReachTheExpectedAnswers) {
    for (const auto &vector : Fixture().at("responses")) {
        SCOPED_TRACE(vector.at("case").get<std::string>());
        const auto answer = iggy::MetadataChanges::FromBytes(FromHex(vector.at("response_hex")));
        EXPECT_EQ(AnswerJson(answer), vector.at("expected"));
    }
}

TEST(MetadataChangesFixtureTest, GivenUnknownKindAndGroupWhenDecodingShouldKeepCodesAndRawBody) {
    for (const auto &vector : Fixture().at("responses")) {
        if (vector.at("case") != "unknown_kind_and_group") {
            continue;
        }
        const auto answer = iggy::MetadataChanges::FromBytes(FromHex(vector.at("response_hex")));
        ASSERT_EQ(answer.Records().size(), 2u);
        for (const auto &record : answer.Records()) {
            EXPECT_FALSE(record.Kind().has_value());
            EXPECT_FALSE(record.Group().has_value());
            const auto *unknown = std::get_if<iggy::MetadataUnknownChange>(&record.Change());
            ASSERT_NE(unknown, nullptr);
            EXPECT_EQ(unknown->Body(), (std::vector<std::uint8_t>{0xde, 0xad, 0xbe, 0xef}));
        }
        return;
    }
    FAIL() << "No unknown_kind_and_group vector";
}

TEST(MetadataChangesFixtureTest, GivenLargeValuesWhenDecodingShouldKeepEveryBit) {
    const auto &vector = Fixture().at("large_values");
    const auto record  = iggy::MetadataChangeRecord::FromBytes(FromHex(vector.at("record_hex")));
    EXPECT_EQ(std::to_string(record.Op()), vector.at("op").get<std::string>());
    EXPECT_EQ(record.Ordinal(), vector.at("ordinal").get<std::uint32_t>());
    EXPECT_EQ(std::to_string(record.Timestamp()), vector.at("timestamp").get<std::string>());
    EXPECT_EQ(ToHex(record.Checksum()), vector.at("checksum").get<std::string>());
    const auto *session = std::get_if<iggy::MetadataSession>(&record.Change());
    ASSERT_NE(session, nullptr);
    EXPECT_EQ(std::to_string(session->UserCreatedAt()), vector.at("user_created_at").get<std::string>());
    EXPECT_EQ(std::to_string(session->Session()), vector.at("session").get<std::string>());
    EXPECT_EQ(ToHex(session->ClientId()), vector.at("client_id").get<std::string>());
}

TEST(MetadataChangesFixtureTest, GivenCursorVectorsWhenParsingShouldMatchTheirFieldsAndText) {
    const auto &constants = Fixture().at("constants");
    for (const auto &vector : Fixture().at("cursors")) {
        SCOPED_TRACE(vector.at("case").get<std::string>());
        const auto text   = vector.at("text").get<std::string>();
        const auto cursor = iggy::MetadataChangeCursor::Parse(text);
        EXPECT_EQ(ToHex(cursor.LogId()), vector.at("log_id").get<std::string>());
        EXPECT_EQ(cursor.Op(), vector.at("op").get<std::uint64_t>());
        EXPECT_EQ(ToHex(cursor.Checksum()), vector.at("checksum").get<std::string>());
        EXPECT_EQ(cursor.Ordinal(), vector.at("ordinal").get<std::uint32_t>());
        EXPECT_EQ(cursor.IsZero(), cursor.Op() == 0);
        EXPECT_EQ(cursor.ToString(), text);
        EXPECT_EQ(cursor.ToString().size(), constants.at("cursor_text_length").get<std::size_t>());
        EXPECT_EQ(cursor.ToString().rfind(constants.at("cursor_prefix").get<std::string>(), 0), 0u);
        EXPECT_EQ(cursor, iggy::MetadataChangeCursor::Parse(cursor.ToString()));
    }
}

TEST(MetadataChangesFixtureTest, GivenPageTokenVectorsWhenParsingShouldMatchTheirFieldsAndText) {
    for (const auto &vector : Fixture().at("page_tokens")) {
        SCOPED_TRACE(vector.at("case").get<std::string>());
        const auto text  = vector.at("text").get<std::string>();
        const auto token = iggy::MetadataCurrentPageToken::Parse(text);
        EXPECT_EQ(token.ToString(), text);
        EXPECT_EQ(token.Anchor().ToString(), vector.at("anchor").get<std::string>());
        EXPECT_EQ(token.RequiredHead(), vector.at("required_head").get<std::uint64_t>());
        std::uint32_t mask = 0;
        for (const auto group : token.Groups()) {
            mask |= 1U << (static_cast<std::uint32_t>(group) - 1);
        }
        EXPECT_EQ(mask, vector.at("groups_mask").get<std::uint32_t>());
        EXPECT_EQ(iggy::to_string(token.LastGroup()), vector.at("group").get<std::string>());
        EXPECT_EQ(ToHex(token.LastKey()), vector.at("last_key_hex").get<std::string>());
    }
}

TEST(MetadataChangesFixtureTest, GivenRequestVectorsWhenEncodingShouldValidateAndMatchTheRequestBytes) {
    for (const auto &vector : Fixture().at("requests")) {
        SCOPED_TRACE(vector.at("case").get<std::string>());
        const auto query = QueryFrom(vector.at("input"));
        EXPECT_NO_THROW(query.Validate());
        EXPECT_EQ(ToHex(query.ToBytes()), vector.at("request_hex").get<std::string>());
    }
}

TEST(MetadataChangesFixtureTest, GivenRefusedCursorAndTokenTextWhenParsingShouldThrowInvalidCommandWithoutTheText) {
    const auto expect_refused = [](const std::string &text, const std::function<void()> &parse) {
        EXPECT_TRUE(ThrowsIggyError(parse, kInvalidCommand, "invalid_command"));
        try {
            parse();
        } catch (const iggy::IggyException &error) {
            EXPECT_EQ(std::string(error.what()).find(text), std::string::npos);
        }
    };
    const auto &refused = Fixture().at("refused");
    for (const auto &vector : refused.at("cursors")) {
        SCOPED_TRACE(vector.at("case").get<std::string>());
        const auto text = vector.at("text").get<std::string>();
        expect_refused(text, [&text] { iggy::MetadataChangeCursor::Parse(text); });
    }
    for (const auto &vector : refused.at("page_tokens")) {
        SCOPED_TRACE(vector.at("case").get<std::string>());
        const auto text = vector.at("text").get<std::string>();
        expect_refused(text, [&text] { iggy::MetadataCurrentPageToken::Parse(text); });
    }
}

TEST(MetadataChangesFixtureTest, GivenNonUtf8CursorAndTokenTextWhenParsingShouldThrowInvalidCommand) {
    const std::string cursor_text = "mc1_" + std::string(58, 'A') + "\xff";
    const std::string token_text  = "cs1_\xff";
    EXPECT_TRUE(ThrowsIggyError([&cursor_text] { iggy::MetadataChangeCursor::Parse(cursor_text); }, kInvalidCommand,
                                "invalid_command"));
    EXPECT_TRUE(ThrowsIggyError([&token_text] { iggy::MetadataCurrentPageToken::Parse(token_text); }, kInvalidCommand,
                                "invalid_command"));
}

TEST(MetadataChangesFixtureTest, GivenRefusedQueriesWhenValidatingShouldThrowInvalidCommand) {
    for (const auto &vector : Fixture().at("refused").at("queries")) {
        SCOPED_TRACE(vector.at("case").get<std::string>());
        const auto query = QueryFrom(vector.at("input"));
        EXPECT_TRUE(ThrowsIggyError([&query] { query.Validate(); }, kInvalidCommand, "invalid_command"));
    }
}

TEST(MetadataChangesFixtureTest, GivenRefusedRecordBodiesWhenDecodingShouldThrowInvalidFormat) {
    for (const auto &vector : Fixture().at("refused").at("record_bodies")) {
        SCOPED_TRACE(vector.at("case").get<std::string>());
        const auto bytes = RecordEnvelope(vector.at("group").get<std::uint8_t>(), vector.at("kind").get<std::uint8_t>(),
                                          FromHex(vector.at("body_hex")));
        EXPECT_TRUE(ThrowsIggyError([&bytes] { iggy::MetadataChangeRecord::FromBytes(bytes); }, kInvalidFormat,
                                    "invalid_format"));
    }
}

// Refused request bytes and HTTP queries are left out because the Rust SDK under this one parses them.
TEST(MetadataChangesFixtureTest, GivenRefusedResponsesWhenDecodingShouldThrowInvalidFormat) {
    for (const auto &vector : Fixture().at("refused").at("responses")) {
        SCOPED_TRACE(vector.at("case").get<std::string>());
        const auto bytes = FromHex(vector.at("hex"));
        EXPECT_TRUE(
            ThrowsIggyError([&bytes] { iggy::MetadataChanges::FromBytes(bytes); }, kInvalidFormat, "invalid_format"));
    }
}

TEST(MetadataChangesFixtureTest, GivenGroupCodeThisBuildDoesNotKnowWhenEncodingShouldThrowInvalidCommand) {
    const auto query = iggy::MetadataChangesQuery::Earliest().SetGroups({static_cast<iggy::MetadataChangeGroup>(9)});
    EXPECT_TRUE(ThrowsIggyError([&query] { static_cast<void>(query.ToBytes()); }, kInvalidCommand, "invalid_command"));
    EXPECT_TRUE(ThrowsIggyError([&query] { query.Validate(); }, kInvalidCommand, "invalid_command"));
}

TEST(MetadataChangesFixtureTest, GivenReadmeSampleWhenCompilingShouldMatchThePublicApi) {
    using Groups              = std::vector<iggy::MetadataChangeGroup>;
    using ApplyStreamFunction = std::function<void(iggy::MetadataChangeKind, const iggy::MetadataStreamChange &)>;
    using StageItemFunction   = std::function<void(const iggy::MetadataCurrentItem &)>;
    using ClearStagedFunction = std::function<void()>;
    // Never invoked, they only have to compile, so the README sample cannot drift from the API.
    const auto rebuild = [](iggy::IggyBlockingClient &client, const Groups &groups,
                            const ClearStagedFunction &ClearStaged, const StageItemFunction &StageItem) {
        ClearStaged();
        auto query = iggy::MetadataChangesQuery::Current().SetGroups(groups);
        while (true) {
            const auto page = client.GetMetadataChanges(query);
            switch (page.Status()) {
                case iggy::MetadataChangeStatus::Ok:
                    break;
                case iggy::MetadataChangeStatus::Behind:
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                    continue;
                case iggy::MetadataChangeStatus::Expired:
                case iggy::MetadataChangeStatus::Reset:
                    ClearStaged();
                    query = iggy::MetadataChangesQuery::Current().SetGroups(groups);
                    continue;
            }
            for (const auto &item : page.Items()) {
                StageItem(item);
            }
            if (!page.CurrentPageToken()) {
                return iggy::MetadataChangesQuery::After(*page.Next()).SetGroups(groups);
            }
            query = iggy::MetadataChangesQuery::Current(*page.CurrentPageToken()).SetGroups(groups);
        }
    };
    const auto follow = [&rebuild](iggy::IggyBlockingClient &client, const Groups &groups,
                                   const ApplyStreamFunction &ApplyStream, const ClearStagedFunction &ClearStaged,
                                   const StageItemFunction &StageItem) {
        auto query = rebuild(client, groups, ClearStaged, StageItem);
        while (true) {
            const auto page = client.GetMetadataChanges(query);
            switch (page.Status()) {
                case iggy::MetadataChangeStatus::Ok:
                    for (const auto &record : page.Records()) {
                        if (const auto *stream = std::get_if<iggy::MetadataStreamChange>(&record.Change())) {
                            ApplyStream(*record.Kind(), *stream);
                        }
                    }
                    query = iggy::MetadataChangesQuery::After(*page.Next()).SetGroups(groups);
                    if (!page.HasMore()) {
                        std::this_thread::sleep_for(std::chrono::seconds(1));
                    }
                    break;
                case iggy::MetadataChangeStatus::Behind:
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                    break;
                case iggy::MetadataChangeStatus::Expired:
                case iggy::MetadataChangeStatus::Reset:
                    query = rebuild(client, groups, ClearStaged, StageItem);
                    break;
            }
        }
    };
    static_cast<void>(follow);
}
