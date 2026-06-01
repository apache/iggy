// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <chrono>
#include <cstdint>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "lib.rs.h"
#include "tests/common/test_helpers.hpp"

class LowLevelE2E_Topic : public E2ETestFixture {};

TEST_F(LowLevelE2E_Topic, CreateTopicWithAllOptionCombinations) {
    RecordProperty("description",
                   "Creates topics across supported option combinations and verifies they are all returned.");
    const std::string stream_name = GetRandomName();

    iggy::ffi::Client *client = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);

    const std::vector<std::string> compression_algorithms = {"none", "gzip"};
    const std::vector<std::uint8_t> replication_factors   = {0, 1};
    struct ExpiryOption {
        std::string kind;
        std::uint64_t value;
    };
    const std::vector<ExpiryOption> expiry_options = {
        {"server_default", 0},
        {"never_expire", 0},
        {"duration", 1000},
    };
    const std::vector<std::string> max_topic_sizes = {"server_default", "unlimited", "1GiB"};

    std::size_t expected_topics_count = 0;
    std::unordered_set<std::string> expected_topic_names;
    for (const auto &compression_algorithm : compression_algorithms) {
        for (const auto replication_factor : replication_factors) {
            for (const auto &expiry_option : expiry_options) {
                for (const auto &max_topic_size : max_topic_sizes) {
                    const std::string topic_name = GetRandomName();
                    SCOPED_TRACE(
                        "compression=" + compression_algorithm + ", replication=" + std::to_string(replication_factor) +
                        ", expiry_kind=" + expiry_option.kind +
                        ", expiry_value=" + std::to_string(expiry_option.value) + ", max_topic_size=" + max_topic_size);

                    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1,
                                                         compression_algorithm, replication_factor, expiry_option.kind,
                                                         expiry_option.value, max_topic_size));
                    ++expected_topics_count;
                    expected_topic_names.insert(topic_name);
                }
            }
        }
    }

    ASSERT_NO_THROW({
        const auto stream_details = client->get_stream(make_string_identifier(stream_name));
        EXPECT_EQ(stream_details.name, stream_name);
        EXPECT_EQ(stream_details.topics_count, expected_topics_count);
        ASSERT_EQ(stream_details.topics.size(), expected_topics_count);
        for (const auto &topic : stream_details.topics) {
            const std::string topic_name = static_cast<std::string>(topic.name);
            const auto erased            = expected_topic_names.erase(topic_name);
            EXPECT_EQ(erased, 1u) << "Unexpected topic name returned: " << topic_name;
        }
        EXPECT_TRUE(expected_topic_names.empty());
    });
}

TEST_F(LowLevelE2E_Topic, CreateTopicWithBoundaryPartitionsCountValues) {
    RecordProperty("description", "Accepts boundary partition counts and rejects values above the supported maximum.");
    const std::string stream_name                = GetRandomName();
    const std::string zero_partitions_topic_name = GetRandomName();
    const std::string max_partitions_topic_name  = GetRandomName();
    const std::string overflow_topic_name        = GetRandomName();

    iggy::ffi::Client *client = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);

    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), zero_partitions_topic_name, 0, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), max_partitions_topic_name, 1000, "none",
                                         0, "server_default", 0, "server_default"));
    ASSERT_THROW(client->create_topic(make_string_identifier(stream_name), overflow_topic_name, 1001, "none", 0,
                                      "server_default", 0, "server_default"),
                 std::exception);

    const auto stream_details = client->get_stream(make_string_identifier(stream_name));
    EXPECT_EQ(stream_details.topics_count, 2u);

    std::unordered_map<std::string, std::uint32_t> topic_partitions;
    for (const auto &topic : stream_details.topics) {
        const std::string topic_name = static_cast<std::string>(topic.name);
        topic_partitions[topic_name] = topic.partitions_count;
    }

    EXPECT_EQ(topic_partitions.size(), 2u);
    EXPECT_EQ(topic_partitions[zero_partitions_topic_name], 0u);
    EXPECT_EQ(topic_partitions[max_partitions_topic_name], 1000u);
}

TEST_F(LowLevelE2E_Topic, CreateTopicWithInvalidNamesThrows) {
    RecordProperty("description", "Rejects invalid topic names and accepts the maximum allowed name length.");
    const std::string stream_name = GetRandomName();

    iggy::ffi::Client *client = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);

    const std::string illegal_topic_names[] = {
        "",
        std::string(256, 'b'),
    };
    for (const auto &topic_name : illegal_topic_names) {
        SCOPED_TRACE(topic_name);
        ASSERT_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                          "server_default", 0, "server_default"),
                     std::exception);
    }

    const std::string max_length_name(255, 'a');
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), max_length_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
}

TEST_F(LowLevelE2E_Topic, CreateDuplicateTopicThrows) {
    RecordProperty("description", "Rejects creating a duplicate topic within the same stream.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    iggy::ffi::Client *client = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0, "server_default",
                                      0, "server_default"),
                 std::exception);
}

TEST_F(LowLevelE2E_Topic, CreateSameTopicNameInDifferentStreamsSucceeds) {
    RecordProperty("description", "Allows the same topic name to be created in different streams.");
    const std::string first_stream_name  = GetRandomName();
    const std::string second_stream_name = GetRandomName();
    const std::string topic_name         = GetRandomName();

    iggy::ffi::Client *client = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(first_stream_name));
    TrackStream(first_stream_name);
    ASSERT_NO_THROW(client->create_stream(second_stream_name));
    TrackStream(second_stream_name);

    ASSERT_NO_THROW(client->create_topic(make_string_identifier(first_stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(second_stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
}

TEST_F(LowLevelE2E_Topic, CreateTopicWithInvalidOptionsThrows) {
    RecordProperty("description", "Rejects topic creation requests that use invalid option values.");
    const std::string stream_name                    = GetRandomName();
    const std::string invalid_compression_topic_name = GetRandomName();
    const std::string invalid_expiry_topic_name      = GetRandomName();
    const std::string invalid_max_size_topic_name    = GetRandomName();

    iggy::ffi::Client *client = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);

    ASSERT_THROW(client->create_topic(make_string_identifier(stream_name), invalid_compression_topic_name, 1,
                                      "invalid-compression", 0, "server_default", 0, "server_default"),
                 std::exception);
    ASSERT_THROW(client->create_topic(make_string_identifier(stream_name), invalid_expiry_topic_name, 1, "none", 0,
                                      "invalid-expiry-kind", 0, "server_default"),
                 std::exception);
    ASSERT_THROW(client->create_topic(make_string_identifier(stream_name), invalid_max_size_topic_name, 1, "none", 0,
                                      "server_default", 0, "not-a-size"),
                 std::exception);
}

TEST_F(LowLevelE2E_Topic, CreateTopicWithMaxTopicSizeBelowSegmentSizeThrows) {
    RecordProperty("description",
                   "Rejects topic creation when the maximum topic size is smaller than the segment size.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    iggy::ffi::Client *client = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0, "server_default",
                                      0, "1024"),
                 std::exception);
}

TEST_F(LowLevelE2E_Topic, CreateTopicOnNonExistentStreamThrows) {
    RecordProperty("description", "Throws when creating a topic on a stream that does not exist.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    iggy::ffi::Client *client = GetLoggedInClient();

    ASSERT_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0, "server_default",
                                      0, "server_default"),
                 std::exception);
}

TEST_F(LowLevelE2E_Topic, CreateTopicAfterStreamDeletionThrows) {
    RecordProperty("description", "Throws when creating a topic after its stream has been deleted.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    iggy::ffi::Client *client = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->delete_stream(make_string_identifier(stream_name)));
    ForgetTrackedStream(stream_name);

    ASSERT_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0, "server_default",
                                      0, "server_default"),
                 std::exception);
}

TEST_F(LowLevelE2E_Topic, CreateTopicWithInvalidStreamIdentifierThrows) {
    RecordProperty("description", "Rejects topic creation requests that use invalid stream identifier formats.");
    const std::string stream_name       = GetRandomName();
    const std::string first_topic_name  = GetRandomName();
    const std::string second_topic_name = GetRandomName();

    iggy::ffi::Client *client = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);

    iggy::ffi::Identifier invalid_kind_id;
    invalid_kind_id.kind   = "invalid";
    invalid_kind_id.length = 4;
    invalid_kind_id.value  = {1, 0, 0, 0};
    ASSERT_THROW(client->create_topic(std::move(invalid_kind_id), first_topic_name, 1, "none", 0, "server_default", 0,
                                      "server_default"),
                 std::exception);

    iggy::ffi::Identifier invalid_numeric_id;
    invalid_numeric_id.kind   = "numeric";
    invalid_numeric_id.length = 1;
    invalid_numeric_id.value.push_back(1);
    ASSERT_THROW(client->create_topic(std::move(invalid_numeric_id), second_topic_name, 1, "none", 0, "server_default",
                                      0, "server_default"),
                 std::exception);
}

TEST_F(LowLevelE2E_Topic, CreateTopicBeforeLoginThrows) {
    RecordProperty("description", "Throws when topic creation is attempted from an unauthenticated client.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    iggy::ffi::Client *client = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);

    iggy::ffi::Client *unauthenticated_client = GetLoggedOutClient();

    ASSERT_NO_THROW(unauthenticated_client->connect());
    ASSERT_THROW(unauthenticated_client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                                      "server_default", 0, "server_default"),
                 std::exception);
}

TEST_F(LowLevelE2E_Topic, PurgeTopicOnNonExistentStreamThrows) {
    RecordProperty("description", "Throws when purging a topic on a stream that does not exist.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    iggy::ffi::Client *client = GetLoggedInClient();

    ASSERT_THROW(client->purge_topic(make_string_identifier(stream_name), make_string_identifier(topic_name)),
                 std::exception);
}

TEST_F(LowLevelE2E_Topic, PurgeTopicAfterStreamDeletionThrows) {
    RecordProperty("description", "Throws when purging a topic after its stream has been deleted.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    iggy::ffi::Client *client = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->delete_stream(make_string_identifier(stream_name)));
    ForgetTrackedStream(stream_name);

    ASSERT_THROW(client->purge_topic(make_string_identifier(stream_name), make_string_identifier(topic_name)),
                 std::exception);
}

TEST_F(LowLevelE2E_Topic, PurgeTopicOnNonExistentTopicThrows) {
    RecordProperty("description", "Throws when purging a topic that does not exist.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    iggy::ffi::Client *client = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);

    ASSERT_THROW(client->purge_topic(make_string_identifier(stream_name), make_string_identifier(topic_name)),
                 std::exception);
}

TEST_F(LowLevelE2E_Topic, PurgeTopicWithInvalidStreamIdentifierThrows) {
    RecordProperty("description", "Rejects topic purge requests that use invalid stream identifier formats.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    iggy::ffi::Client *client = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));

    iggy::ffi::Identifier invalid_kind_id;
    invalid_kind_id.kind   = "invalid";
    invalid_kind_id.length = 4;
    invalid_kind_id.value  = {1, 0, 0, 0};
    ASSERT_THROW(client->purge_topic(std::move(invalid_kind_id), make_string_identifier(topic_name)), std::exception);

    iggy::ffi::Identifier invalid_numeric_id;
    invalid_numeric_id.kind   = "numeric";
    invalid_numeric_id.length = 1;
    invalid_numeric_id.value.push_back(1);
    ASSERT_THROW(client->purge_topic(std::move(invalid_numeric_id), make_string_identifier(topic_name)),
                 std::exception);
}

TEST_F(LowLevelE2E_Topic, PurgeTopicWithInvalidTopicIdentifierThrows) {
    RecordProperty("description", "Rejects topic purge requests that use invalid topic identifier formats.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    iggy::ffi::Client *client = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));

    iggy::ffi::Identifier invalid_kind_id;
    invalid_kind_id.kind   = "invalid";
    invalid_kind_id.length = 4;
    invalid_kind_id.value  = {1, 0, 0, 0};
    ASSERT_THROW(client->purge_topic(make_string_identifier(stream_name), std::move(invalid_kind_id)), std::exception);

    iggy::ffi::Identifier invalid_numeric_id;
    invalid_numeric_id.kind   = "numeric";
    invalid_numeric_id.length = 1;
    invalid_numeric_id.value.push_back(1);
    ASSERT_THROW(client->purge_topic(make_string_identifier(stream_name), std::move(invalid_numeric_id)),
                 std::exception);
}

TEST_F(LowLevelE2E_Topic, PurgeTopicPreservesTopicMetadata) {
    RecordProperty("description", "Preserves topic metadata after purging its messages.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    iggy::ffi::Client *client = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(
        client->create_topic(make_string_identifier(stream_name), topic_name, 3, "gzip", 1, "duration", 1000, "1GiB"));

    auto stream_before_purge = client->get_stream(make_string_identifier(stream_name));
    ASSERT_EQ(stream_before_purge.topics.size(), 1u);
    const auto topic_before_messages = stream_before_purge.topics.front();

    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    messages.push_back(iggy::ffi::make_message(to_payload("preserve-topic-metadata")));
    ASSERT_NO_THROW(client->send_messages(make_numeric_identifier(stream_before_purge.id),
                                          make_numeric_identifier(topic_before_messages.id), "partition_id",
                                          partition_id_bytes(1), std::move(messages)));

    stream_before_purge = client->get_stream(make_string_identifier(stream_name));
    ASSERT_EQ(stream_before_purge.topics.size(), 1u);
    const auto topic_with_messages = stream_before_purge.topics.front();
    EXPECT_GT(topic_with_messages.messages_count, 0u);
    EXPECT_GT(topic_with_messages.size_bytes, 0u);

    ASSERT_NO_THROW(client->purge_topic(make_string_identifier(stream_name), make_string_identifier(topic_name)));

    const auto stream_after_purge = client->get_stream(make_string_identifier(stream_name));
    ASSERT_EQ(stream_after_purge.topics.size(), 1u);
    const auto topic_after_purge = stream_after_purge.topics.front();

    EXPECT_EQ(topic_after_purge.id, topic_with_messages.id);
    EXPECT_EQ(topic_after_purge.created_at, topic_with_messages.created_at);
    EXPECT_EQ(topic_after_purge.name, topic_with_messages.name);
    EXPECT_EQ(topic_after_purge.message_expiry, topic_with_messages.message_expiry);
    EXPECT_EQ(topic_after_purge.compression_algorithm, topic_with_messages.compression_algorithm);
    EXPECT_EQ(topic_after_purge.max_topic_size, topic_with_messages.max_topic_size);
    EXPECT_EQ(topic_after_purge.replication_factor, topic_with_messages.replication_factor);
    EXPECT_EQ(topic_after_purge.partitions_count, topic_with_messages.partitions_count);
}

TEST_F(LowLevelE2E_Topic, PurgeTopicRemovesOnlyTargetTopicMessages) {
    RecordProperty("description", "Purges one topic's messages without affecting the other topics in the stream.");
    const std::string stream_name       = GetRandomName();
    const std::string first_topic_name  = GetRandomName();
    const std::string second_topic_name = GetRandomName();

    iggy::ffi::Client *client = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), first_topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), second_topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));

    const auto created_stream = client->get_stream(make_string_identifier(stream_name));
    ASSERT_EQ(created_stream.topics.size(), 2u);

    std::uint32_t first_topic_id  = 0;
    std::uint32_t second_topic_id = 0;
    bool first_topic_found        = false;
    bool second_topic_found       = false;
    for (const auto &topic : created_stream.topics) {
        const std::string topic_name = static_cast<std::string>(topic.name);
        if (topic_name == first_topic_name) {
            first_topic_id    = topic.id;
            first_topic_found = true;
        } else if (topic_name == second_topic_name) {
            second_topic_id    = topic.id;
            second_topic_found = true;
        }
    }
    ASSERT_TRUE(first_topic_found);
    ASSERT_TRUE(second_topic_found);

    rust::Vec<iggy::ffi::IggyMessageToSend> first_topic_messages;
    for (std::uint32_t i = 0; i < 3; ++i) {
        first_topic_messages.push_back(iggy::ffi::make_message(to_payload("purge-topic-first-" + std::to_string(i))));
    }
    ASSERT_NO_THROW(client->send_messages(make_numeric_identifier(created_stream.id),
                                          make_numeric_identifier(first_topic_id), "partition_id",
                                          partition_id_bytes(0), std::move(first_topic_messages)));

    rust::Vec<iggy::ffi::IggyMessageToSend> second_topic_messages;
    for (std::uint32_t i = 0; i < 2; ++i) {
        second_topic_messages.push_back(iggy::ffi::make_message(to_payload("purge-topic-second-" + std::to_string(i))));
    }
    ASSERT_NO_THROW(client->send_messages(make_numeric_identifier(created_stream.id),
                                          make_numeric_identifier(second_topic_id), "partition_id",
                                          partition_id_bytes(0), std::move(second_topic_messages)));

    const auto stream_before_purge = client->get_stream(make_string_identifier(stream_name));
    EXPECT_EQ(stream_before_purge.messages_count, 5u);
    EXPECT_GT(stream_before_purge.size_bytes, 0u);

    std::unordered_map<std::string, std::uint64_t> messages_before_purge;
    for (const auto &topic : stream_before_purge.topics) {
        messages_before_purge[static_cast<std::string>(topic.name)] = topic.messages_count;
    }
    EXPECT_EQ(messages_before_purge[first_topic_name], 3u);
    EXPECT_EQ(messages_before_purge[second_topic_name], 2u);

    ASSERT_NO_THROW(client->purge_topic(make_string_identifier(stream_name), make_string_identifier(first_topic_name)));

    const auto stream_after_purge = client->get_stream(make_string_identifier(stream_name));
    EXPECT_EQ(stream_after_purge.topics_count, 2u);
    EXPECT_EQ(stream_after_purge.messages_count, 2u);
    EXPECT_GT(stream_after_purge.size_bytes, 0u);
    EXPECT_LT(stream_after_purge.size_bytes, stream_before_purge.size_bytes);

    std::unordered_map<std::string, std::uint64_t> messages_after_purge;
    std::unordered_map<std::string, std::uint64_t> sizes_after_purge;
    for (const auto &topic : stream_after_purge.topics) {
        const std::string topic_name     = static_cast<std::string>(topic.name);
        messages_after_purge[topic_name] = topic.messages_count;
        sizes_after_purge[topic_name]    = topic.size_bytes;
    }
    EXPECT_EQ(messages_after_purge[first_topic_name], 0u);
    EXPECT_EQ(sizes_after_purge[first_topic_name], 0u);
    EXPECT_EQ(messages_after_purge[second_topic_name], 2u);
    EXPECT_GT(sizes_after_purge[second_topic_name], 0u);
}

TEST_F(LowLevelE2E_Topic, PurgeTopicAcrossMultiplePartitionsClearsAllPartitions) {
    RecordProperty("description", "Purges all messages from every partition in the topic.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    iggy::ffi::Client *client = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 3, "none", 0,
                                         "server_default", 0, "server_default"));

    const auto created_stream = client->get_stream(make_string_identifier(stream_name));
    ASSERT_EQ(created_stream.topics.size(), 1u);
    const std::uint32_t topic_id = created_stream.topics.front().id;

    for (std::uint32_t partition_id = 0; partition_id < 3; ++partition_id) {
        rust::Vec<iggy::ffi::IggyMessageToSend> messages;
        for (std::uint32_t i = 0; i < 2; ++i) {
            messages.push_back(iggy::ffi::make_message(
                to_payload("purge-topic-partition-" + std::to_string(partition_id) + "-" + std::to_string(i))));
        }
        ASSERT_NO_THROW(client->send_messages(make_numeric_identifier(created_stream.id),
                                              make_numeric_identifier(topic_id), "partition_id",
                                              partition_id_bytes(partition_id), std::move(messages)));
    }

    const auto stream_before_purge = client->get_stream(make_string_identifier(stream_name));
    EXPECT_EQ(stream_before_purge.messages_count, 6u);
    ASSERT_EQ(stream_before_purge.topics.size(), 1u);
    EXPECT_EQ(stream_before_purge.topics.front().partitions_count, 3u);
    EXPECT_EQ(stream_before_purge.topics.front().messages_count, 6u);

    ASSERT_NO_THROW(client->purge_topic(make_string_identifier(stream_name), make_string_identifier(topic_name)));

    const auto stream_after_purge = client->get_stream(make_string_identifier(stream_name));
    EXPECT_EQ(stream_after_purge.messages_count, 0u);
    ASSERT_EQ(stream_after_purge.topics.size(), 1u);
    EXPECT_EQ(stream_after_purge.topics.front().partitions_count, 3u);
    EXPECT_EQ(stream_after_purge.topics.front().messages_count, 0u);
    EXPECT_EQ(stream_after_purge.topics.front().size_bytes, 0u);
}

TEST_F(LowLevelE2E_Topic, PurgeTopicThenSendMessagesAgainSucceeds) {
    RecordProperty("description", "Allows sending fresh messages again after purging the topic.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    iggy::ffi::Client *client = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));

    const auto created_stream = client->get_stream(make_string_identifier(stream_name));
    ASSERT_EQ(created_stream.topics.size(), 1u);
    const std::uint32_t topic_id = created_stream.topics.front().id;

    rust::Vec<iggy::ffi::IggyMessageToSend> first_batch;
    first_batch.push_back(iggy::ffi::make_message(to_payload("before-topic-purge")));
    ASSERT_NO_THROW(client->send_messages(make_numeric_identifier(created_stream.id), make_numeric_identifier(topic_id),
                                          "partition_id", partition_id_bytes(0), std::move(first_batch)));

    ASSERT_NO_THROW(client->purge_topic(make_string_identifier(stream_name), make_string_identifier(topic_name)));

    rust::Vec<iggy::ffi::IggyMessageToSend> second_batch;
    second_batch.push_back(iggy::ffi::make_message(to_payload("after-topic-purge-0")));
    second_batch.push_back(iggy::ffi::make_message(to_payload("after-topic-purge-1")));
    ASSERT_NO_THROW(client->send_messages(make_numeric_identifier(created_stream.id), make_numeric_identifier(topic_id),
                                          "partition_id", partition_id_bytes(0), std::move(second_batch)));

    const auto stream_after_resend = client->get_stream(make_string_identifier(stream_name));
    EXPECT_EQ(stream_after_resend.messages_count, 2u);
    ASSERT_EQ(stream_after_resend.topics.size(), 1u);
    EXPECT_EQ(stream_after_resend.topics.front().messages_count, 2u);
    EXPECT_GT(stream_after_resend.topics.front().size_bytes, 0u);
}

TEST_F(LowLevelE2E_Topic, PurgeTopicTwiceKeepsTargetTopicEmptyAndOtherTopicsUntouched) {
    RecordProperty("description",
                   "Allows purging the same topic twice and keeps the target topic empty without affecting siblings.");
    const std::string stream_name       = GetRandomName();
    const std::string first_topic_name  = GetRandomName();
    const std::string second_topic_name = GetRandomName();

    iggy::ffi::Client *client = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), first_topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), second_topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));

    const auto created_stream = client->get_stream(make_string_identifier(stream_name));
    ASSERT_EQ(created_stream.topics.size(), 2u);

    std::uint32_t first_topic_id  = 0;
    std::uint32_t second_topic_id = 0;
    bool first_topic_found        = false;
    bool second_topic_found       = false;
    for (const auto &topic : created_stream.topics) {
        const std::string topic_name = static_cast<std::string>(topic.name);
        if (topic_name == first_topic_name) {
            first_topic_id    = topic.id;
            first_topic_found = true;
        } else if (topic_name == second_topic_name) {
            second_topic_id    = topic.id;
            second_topic_found = true;
        }
    }
    ASSERT_TRUE(first_topic_found);
    ASSERT_TRUE(second_topic_found);

    rust::Vec<iggy::ffi::IggyMessageToSend> first_topic_messages;
    for (std::uint32_t i = 0; i < 3; ++i) {
        first_topic_messages.push_back(
            iggy::ffi::make_message(to_payload("purge-topic-twice-first-" + std::to_string(i))));
    }
    ASSERT_NO_THROW(client->send_messages(make_numeric_identifier(created_stream.id),
                                          make_numeric_identifier(first_topic_id), "partition_id",
                                          partition_id_bytes(0), std::move(first_topic_messages)));

    rust::Vec<iggy::ffi::IggyMessageToSend> second_topic_messages;
    for (std::uint32_t i = 0; i < 2; ++i) {
        second_topic_messages.push_back(
            iggy::ffi::make_message(to_payload("purge-topic-twice-second-" + std::to_string(i))));
    }
    ASSERT_NO_THROW(client->send_messages(make_numeric_identifier(created_stream.id),
                                          make_numeric_identifier(second_topic_id), "partition_id",
                                          partition_id_bytes(0), std::move(second_topic_messages)));

    ASSERT_NO_THROW(client->purge_topic(make_string_identifier(stream_name), make_string_identifier(first_topic_name)));
    const auto stream_after_first_purge = client->get_stream(make_string_identifier(stream_name));
    EXPECT_EQ(stream_after_first_purge.topics_count, 2u);
    EXPECT_EQ(stream_after_first_purge.messages_count, 2u);

    std::unordered_map<std::string, std::uint64_t> messages_after_first_purge;
    std::unordered_map<std::string, std::uint64_t> sizes_after_first_purge;
    for (const auto &topic : stream_after_first_purge.topics) {
        const std::string topic_name           = static_cast<std::string>(topic.name);
        messages_after_first_purge[topic_name] = topic.messages_count;
        sizes_after_first_purge[topic_name]    = topic.size_bytes;
    }
    EXPECT_EQ(messages_after_first_purge[first_topic_name], 0u);
    EXPECT_EQ(sizes_after_first_purge[first_topic_name], 0u);
    EXPECT_EQ(messages_after_first_purge[second_topic_name], 2u);
    EXPECT_GT(sizes_after_first_purge[second_topic_name], 0u);

    ASSERT_NO_THROW(client->purge_topic(make_string_identifier(stream_name), make_string_identifier(first_topic_name)));
    const auto stream_after_second_purge = client->get_stream(make_string_identifier(stream_name));
    EXPECT_EQ(stream_after_second_purge.topics_count, 2u);
    EXPECT_EQ(stream_after_second_purge.messages_count, 2u);

    std::unordered_map<std::string, std::uint64_t> messages_after_second_purge;
    std::unordered_map<std::string, std::uint64_t> sizes_after_second_purge;
    for (const auto &topic : stream_after_second_purge.topics) {
        const std::string topic_name            = static_cast<std::string>(topic.name);
        messages_after_second_purge[topic_name] = topic.messages_count;
        sizes_after_second_purge[topic_name]    = topic.size_bytes;
    }
    EXPECT_EQ(messages_after_second_purge[first_topic_name], 0u);
    EXPECT_EQ(sizes_after_second_purge[first_topic_name], 0u);
    EXPECT_EQ(messages_after_second_purge[second_topic_name], 2u);
    EXPECT_GT(sizes_after_second_purge[second_topic_name], 0u);
}

TEST_F(LowLevelE2E_Topic, PurgeTopicBeforeLoginThrows) {
    RecordProperty("description", "Throws when topic purge is attempted before authentication.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    iggy::ffi::Client *client = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));

    iggy::ffi::Client *unauthenticated_client = GetLoggedOutClient();

    ASSERT_THROW(
        unauthenticated_client->purge_topic(make_string_identifier(stream_name), make_string_identifier(topic_name)),
        std::exception);
    ASSERT_NO_THROW(unauthenticated_client->connect());
    ASSERT_THROW(
        unauthenticated_client->purge_topic(make_string_identifier(stream_name), make_string_identifier(topic_name)),
        std::exception);
}
