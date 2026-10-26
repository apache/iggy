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

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include <gtest/gtest.h>

#include "tests/e2e/test_helpers.hpp"

struct MetadataChangesTransport {
    std::string name;
    // Empty selects the TCP builder.
    std::string scheme;
    std::string address;

    [[nodiscard]] std::string ConnectionString(const std::string &username, const std::string &password) const {
        return scheme + "://" + username + ":" + password + "@" + address;
    }
};

namespace {

constexpr std::uint32_t kRootUserId     = 0;
constexpr std::uint32_t kInvalidCommand = 3;
constexpr std::uint32_t kUnauthorized   = 41;
constexpr int kFollowAttempts           = 100;
constexpr auto kFollowInterval          = std::chrono::milliseconds(100);
constexpr std::size_t kKeyLength        = 32;
constexpr std::uint8_t kKeyByte         = 21;
constexpr std::size_t kFingerprintHex   = 16;
constexpr const char *kHiddenOptions[]  = {"encryption_key", "encryption_key_wrapped", "critical_keys"};

const std::vector<iggy::MetadataChangeGroup> kStreamGroups{iggy::MetadataChangeGroup::Streams};

std::string PrintTransport(const ::testing::TestParamInfo<MetadataChangesTransport> &info) {
    return info.param.name;
}

// A record's stream ID and creation op, which tells a stream from an earlier one with the same ID.
std::optional<std::pair<std::uint32_t, std::uint64_t>> ChangeStream(const iggy::MetadataChangeRecord &record) {
    return std::visit(
        [](const auto &body) -> std::optional<std::pair<std::uint32_t, std::uint64_t>> {
            using Body = std::decay_t<decltype(body)>;
            if constexpr (std::is_same_v<Body, iggy::MetadataStreamChange> ||
                          std::is_same_v<Body, iggy::MetadataStreamDeleted> ||
                          std::is_same_v<Body, iggy::MetadataTopicChange> ||
                          std::is_same_v<Body, iggy::MetadataTopicDeleted> ||
                          std::is_same_v<Body, iggy::MetadataPartitionsChanged>) {
                return std::make_pair(body.StreamId(), body.StreamCreatedOp());
            } else {
                return std::nullopt;
            }
        },
        record.Change());
}

std::vector<iggy::MetadataChangeKind> KindsOf(const std::vector<iggy::MetadataChangeRecord> &records) {
    std::vector<iggy::MetadataChangeKind> kinds;
    kinds.reserve(records.size());
    for (const auto &record : records) {
        kinds.push_back(record.Kind().value());
    }
    return kinds;
}

std::optional<std::string> OptionText(const iggy::ResourceOptions &options, const std::string &key) {
    for (const auto *entries : {&options.Explicit(), &options.Derived()}) {
        const auto entry = entries->find(key);
        if (entry != entries->end()) {
            const auto &bytes = entry->second.Value();
            return std::string(bytes.begin(), bytes.end());
        }
    }
    return std::nullopt;
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
        return ::testing::AssertionFailure()
               << "expected " << kind << " (" << code << "), got '" << exception.what() << "' with code "
               << exception.Code().value_or(0) << " and kind '" << exception.Kind() << "'";
    }
    return ::testing::AssertionFailure() << "expected " << kind << " (" << code << "), nothing was thrown";
}

}  // namespace

class MetadataChangesTransportFixture : public E2ETestFixture,
                                        public ::testing::WithParamInterface<MetadataChangesTransport> {
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

    static iggy::MetadataChangeCursor LatestCursor(iggy::IggyBlockingClient &client,
                                                   const std::vector<iggy::MetadataChangeGroup> &groups) {
        const auto latest = client.GetMetadataChanges(iggy::MetadataChangesQuery::Latest().SetGroups(groups));
        EXPECT_EQ(latest.Status(), iggy::MetadataChangeStatus::Ok);
        EXPECT_EQ(latest.Phase(), iggy::MetadataChangePhase::Changes);
        EXPECT_TRUE(latest.Records().empty());
        EXPECT_TRUE(latest.LogId().has_value());
        return latest.Next().value();
    }

    // A two phase delete commits its record after the call returns, so an incomplete read polls
    // again. The deferred delete of an earlier stream with the same ID is skipped.
    static std::vector<iggy::MetadataChangeRecord> Follow(iggy::IggyBlockingClient &client,
                                                          iggy::MetadataChangeCursor cursor,
                                                          const std::vector<iggy::MetadataChangeGroup> &groups,
                                                          std::uint32_t stream_id,
                                                          std::size_t count,
                                                          std::uint32_t limit = iggy::MetadataChangesQuery::kMaxLimit) {
        std::vector<iggy::MetadataChangeRecord> mine;
        std::optional<std::uint64_t> stream_created_op;
        for (int attempt = 0; attempt < kFollowAttempts; ++attempt) {
            const auto page =
                client.GetMetadataChanges(iggy::MetadataChangesQuery::After(cursor).SetGroups(groups).SetLimit(limit));
            EXPECT_EQ(page.Status(), iggy::MetadataChangeStatus::Ok);
            EXPECT_LE(page.Records().size(), limit);
            cursor = page.Next().value();
            for (const auto &record : page.Records()) {
                const auto group = record.Group();
                EXPECT_TRUE(group.has_value() && std::find(groups.begin(), groups.end(), *group) != groups.end())
                    << "record of op " << record.Op() << " is outside the requested groups";
                const auto stream = ChangeStream(record);
                if (!stream || stream->first != stream_id) {
                    continue;
                }
                if (!stream_created_op && (record.Kind() == iggy::MetadataChangeKind::StreamCreated ||
                                           record.Kind() == iggy::MetadataChangeKind::TopicCreated)) {
                    stream_created_op = stream->second;
                }
                if (stream->second == stream_created_op) {
                    mine.push_back(record);
                }
            }
            if (page.HasMore()) {
                continue;
            }
            if (mine.size() >= count) {
                break;
            }
            std::this_thread::sleep_for(kFollowInterval);
        }
        return mine;
    }
};

class E2E_MetadataChangesTransport : public MetadataChangesTransportFixture {};

// Needs a server encryption key. The shipped default lane skips every *ResourceOptions* suite.
class E2E_MetadataChangesResourceOptionsTransport : public MetadataChangesTransportFixture {};

INSTANTIATE_TEST_SUITE_P(Transports,
                         E2E_MetadataChangesTransport,
                         ::testing::Values(MetadataChangesTransport{"tcp", "", ""},
                                           MetadataChangesTransport{"quic", "iggy+quic", "127.0.0.1:8080"},
                                           MetadataChangesTransport{"http", "iggy+http", "127.0.0.1:3000"},
                                           MetadataChangesTransport{"websocket", "iggy+ws", "127.0.0.1:8092"}),
                         PrintTransport);

INSTANTIATE_TEST_SUITE_P(Transports,
                         E2E_MetadataChangesResourceOptionsTransport,
                         ::testing::Values(MetadataChangesTransport{"tcp", "", ""},
                                           MetadataChangesTransport{"quic", "iggy+quic", "127.0.0.1:8080"},
                                           MetadataChangesTransport{"http", "iggy+http", "127.0.0.1:3000"},
                                           MetadataChangesTransport{"websocket", "iggy+ws", "127.0.0.1:8092"}),
                         PrintTransport);

TEST_P(E2E_MetadataChangesTransport, GivenLatestCursorWhenCreatingRenamingAndDeletingStreamShouldFollowEachChange) {
    RecordProperty("description",
                   "Holds the latest cursor for the streams group, creates, renames and deletes a stream, and follows "
                   "one record per change in order, each naming the root user as its actor.");
    auto client        = GetTransportClient();
    const auto cursor  = LatestCursor(client, kStreamGroups);
    const auto name    = GetRandomName(40);
    const auto renamed = name + "-renamed";

    const auto stream    = client.CreateStream(name);
    const auto stream_id = iggy::Identifier::Numeric(stream.Id());
    TrackStream(stream.Id());
    client.UpdateStream(stream_id, iggy::StreamUpdateOptions().SetName(renamed));
    client.DeleteStream(stream_id);
    ForgetTrackedStream(stream.Id());

    const auto records = Follow(client, cursor, kStreamGroups, stream.Id(), 3);
    ASSERT_EQ(KindsOf(records), (std::vector<iggy::MetadataChangeKind>{iggy::MetadataChangeKind::StreamCreated,
                                                                       iggy::MetadataChangeKind::StreamUpdated,
                                                                       iggy::MetadataChangeKind::StreamDeleted}));
    EXPECT_EQ(std::get<iggy::MetadataStreamChange>(records[0].Change()).Name(), name);
    EXPECT_EQ(std::get<iggy::MetadataStreamChange>(records[1].Change()).Name(), renamed);
    const auto &deleted = std::get<iggy::MetadataStreamDeleted>(records[2].Change());
    EXPECT_EQ(deleted.Name(), renamed);
    EXPECT_EQ(deleted.StreamCreatedOp(), records[0].Op());
    for (std::size_t index = 0; index < records.size(); ++index) {
        EXPECT_EQ(records[index].ActorUserId(), kRootUserId) << index;
        EXPECT_EQ(records[index].Group(), iggy::MetadataChangeGroup::Streams) << index;
        if (index > 0) {
            EXPECT_GT(records[index].Op(), records[index - 1].Op()) << index;
        }
    }
}

TEST_P(E2E_MetadataChangesTransport, GivenTopicsGroupWhenCreatingStreamAndTopicShouldFollowOnlyTheTopic) {
    RecordProperty("description",
                   "Holds the latest cursor for the topics group, creates a stream and a topic, and follows only the "
                   "topic record.");
    auto client                                         = GetTransportClient();
    const std::vector<iggy::MetadataChangeGroup> groups = {iggy::MetadataChangeGroup::Topics};
    const auto cursor                                   = LatestCursor(client, groups);
    const auto name                                     = GetRandomName(40);

    const auto stream = client.CreateStream(name);
    TrackStream(name);
    client.CreateTopic(iggy::Identifier::Numeric(stream.Id()), "events",
                       iggy::TopicCreateOptions().SetPartitionsCount(1));

    const auto records = Follow(client, cursor, groups, stream.Id(), 1);
    ASSERT_EQ(KindsOf(records), (std::vector<iggy::MetadataChangeKind>{iggy::MetadataChangeKind::TopicCreated}));
    const auto &topic = std::get<iggy::MetadataTopicChange>(records[0].Change());
    EXPECT_EQ(topic.Name(), "events");
    EXPECT_EQ(topic.PartitionsCount(), 1u);
    EXPECT_GT(topic.TopicCreatedOp(), topic.StreamCreatedOp());
    EXPECT_TRUE(topic.Options().has_value());
}

TEST_P(E2E_MetadataChangesTransport, GivenLimitOfOneWhenFollowingShouldPageOneRecordAtATimeInOrder) {
    RecordProperty("description",
                   "Creates three streams after the latest cursor and pages the records one at a time, reaching the "
                   "same records a single large page returns.");
    auto client       = GetTransportClient();
    const auto cursor = LatestCursor(client, kStreamGroups);
    std::set<std::uint32_t> stream_ids;
    for (int index = 0; index < 3; ++index) {
        const auto name = GetRandomName(40);
        stream_ids.insert(client.CreateStream(name).Id());
        TrackStream(name);
    }
    // A deferred delete of an earlier test can commit after the cursor, so only these creates count.
    const auto created_here = [&stream_ids](const iggy::MetadataChangeRecord &record) {
        const auto stream = ChangeStream(record);
        return record.Kind() == iggy::MetadataChangeKind::StreamCreated && stream &&
               stream_ids.count(stream->first) == 1;
    };

    std::vector<std::uint64_t> paged;
    auto position = cursor;
    while (true) {
        const auto page =
            client.GetMetadataChanges(iggy::MetadataChangesQuery::After(position).SetGroups(kStreamGroups).SetLimit(1));
        ASSERT_EQ(page.Status(), iggy::MetadataChangeStatus::Ok);
        ASSERT_LE(page.Records().size(), 1u);
        for (const auto &record : page.Records()) {
            if (created_here(record)) {
                paged.push_back(record.Op());
            }
        }
        position = page.Next().value();
        if (!page.HasMore()) {
            break;
        }
    }

    const auto whole = client.GetMetadataChanges(iggy::MetadataChangesQuery::After(cursor).SetGroups(kStreamGroups));
    std::vector<std::uint64_t> expected;
    for (const auto &record : whole.Records()) {
        if (created_here(record)) {
            expected.push_back(record.Op());
        }
    }
    EXPECT_EQ(expected.size(), stream_ids.size());
    EXPECT_EQ(paged, expected);
    EXPECT_TRUE(std::is_sorted(paged.begin(), paged.end()));
}

TEST_P(E2E_MetadataChangesTransport, GivenStreamsWhenPagingCurrentStateShouldListTheSameStreamsAsTheStreamList) {
    RecordProperty("description",
                   "Pages the current state of the streams group one item at a time, compares it with GetStreams(), "
                   "and follows the change log from the anchor the last page returns.");
    auto client = GetTransportClient();
    for (int index = 0; index < 2; ++index) {
        const auto name = GetRandomName(40);
        client.CreateStream(name);
        TrackStream(name);
    }

    std::map<std::uint32_t, std::string> projected;
    auto query = iggy::MetadataChangesQuery::Current().SetGroups(kStreamGroups).SetLimit(1);
    std::optional<iggy::MetadataChangeCursor> anchor;
    while (!anchor) {
        const auto page = client.GetMetadataChanges(query);
        ASSERT_EQ(page.Status(), iggy::MetadataChangeStatus::Ok);
        ASSERT_EQ(page.Phase(), iggy::MetadataChangePhase::Current);
        ASSERT_TRUE(page.Records().empty());
        ASSERT_LE(page.Items().size(), 1u);
        for (const auto &item : page.Items()) {
            const auto &stream = std::get<iggy::MetadataStreamChange>(item.Value());
            projected.emplace(stream.StreamId(), stream.Name());
        }
        if (page.CurrentPageToken()) {
            query = iggy::MetadataChangesQuery::Current(*page.CurrentPageToken()).SetGroups(kStreamGroups).SetLimit(1);
        } else {
            anchor = page.Next();
            ASSERT_TRUE(anchor.has_value());
        }
    }

    std::map<std::uint32_t, std::string> listed;
    for (const auto &stream : client.GetStreams()) {
        listed.emplace(stream.Id(), stream.Name());
    }
    EXPECT_EQ(projected, listed);
    const auto follow = client.GetMetadataChanges(iggy::MetadataChangesQuery::After(*anchor).SetGroups(kStreamGroups));
    EXPECT_EQ(follow.Status(), iggy::MetadataChangeStatus::Ok);
}

TEST_P(E2E_MetadataChangesTransport, GivenInvalidQueriesWhenReadingShouldRefuseEachWithInvalidCommand) {
    RecordProperty("description",
                   "Refuses limits outside 1 to 1000, since above until, time bounds with latest or current, and a "
                   "current scan without its parent group before anything is sent.");
    auto client        = GetTransportClient();
    const auto queries = {
        iggy::MetadataChangesQuery::Earliest().SetLimit(0),
        iggy::MetadataChangesQuery::Earliest().SetLimit(iggy::MetadataChangesQuery::kMaxLimit + 1),
        iggy::MetadataChangesQuery::Earliest().SetSince(2).SetUntil(1),
        iggy::MetadataChangesQuery::Latest().SetSince(1),
        iggy::MetadataChangesQuery::Current().SetUntil(1),
        iggy::MetadataChangesQuery::Current().SetGroups({iggy::MetadataChangeGroup::Topics}),
    };
    for (const auto &query : queries) {
        EXPECT_TRUE(ThrowsIggyError([&client, &query] { client.GetMetadataChanges(query); }, kInvalidCommand,
                                    "invalid_command"));
    }
}

TEST_P(E2E_MetadataChangesTransport, GivenUserWithoutManageServersWhenReadingShouldBeRefusedAsUnauthorized) {
    RecordProperty("description", "A user with read_servers but not manage_servers is refused with Unauthorized (41).");
    auto root           = GetTransportClient();
    const auto username = GetRandomName(30);
    const std::string password{"changes-secret"};
    CreateUser(root, username, password, iggy::UserStatus::Active,
               iggy::Permissions().SetGlobal(iggy::GlobalPermissions().SetReadServers(true)));

    auto reader = GetTransportClient(username, password);
    EXPECT_TRUE(ThrowsIggyError([&reader] { reader.GetMetadataChanges(iggy::MetadataChangesQuery::Earliest()); },
                                kUnauthorized, "unauthorized"));
}

TEST_P(E2E_MetadataChangesResourceOptionsTransport,
       GivenKeyedStreamWhenFollowingShouldExposeFingerprintsAndNoKeyMaterial) {
    RecordProperty("description",
                   "Creates a stream with its own key and follows its record, which reports the key source and "
                   "fingerprints and carries no key material.");
    auto client       = GetTransportClient();
    const auto cursor = LatestCursor(client, kStreamGroups);
    const auto name   = GetRandomName(40);
    const std::vector<std::uint8_t> key(kKeyLength, kKeyByte);

    const auto stream = client.CreateStream(name, iggy::StreamCreateOptions().SetEncryptionKey(key));
    TrackStream(name);

    const auto records = Follow(client, cursor, kStreamGroups, stream.Id(), 1);
    ASSERT_EQ(KindsOf(records), (std::vector<iggy::MetadataChangeKind>{iggy::MetadataChangeKind::StreamCreated}));
    const auto &options = std::get<iggy::MetadataStreamChange>(records[0].Change()).Options();
    ASSERT_TRUE(options.has_value());
    EXPECT_EQ(OptionText(*options, "encryption_key_source"),
              std::string(iggy::to_string(iggy::StreamEncryptionSource::Stream)));
    EXPECT_EQ(OptionText(*options, "encryption_key_fingerprint"), iggy::EncryptionKeyFingerprint(key));
    const auto kek_fingerprint = OptionText(*options, "encryption_kek_fingerprint");
    ASSERT_TRUE(kek_fingerprint.has_value());
    EXPECT_EQ(kek_fingerprint->size(), kFingerprintHex);
    for (const auto *hidden : kHiddenOptions) {
        EXPECT_EQ(options->Explicit().count(hidden), 0u) << hidden;
        EXPECT_EQ(options->Derived().count(hidden), 0u) << hidden;
    }
}
