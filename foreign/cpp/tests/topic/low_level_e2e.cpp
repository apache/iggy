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

#include <cstdint>
#include <limits>
#include <string>
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
    const std::vector<std::uint8_t> replication_factors   = {0, 1, 27};
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

TEST_F(LowLevelE2E_Topic, DeleteTopicAfterCreate) {
    RecordProperty("description", "Deletes an existing topic after creating it.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    iggy::ffi::Client *client = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));

    ASSERT_NO_THROW(client->delete_topic(make_string_identifier(stream_name), make_string_identifier(topic_name)));

    ASSERT_NO_THROW({
        const auto topics = client->get_topics(make_string_identifier(stream_name));
        EXPECT_TRUE(topics.empty());
    });
}

TEST_F(LowLevelE2E_Topic, DeleteTopicOnNonExistentStreamThrows) {
    RecordProperty("description", "Throws when deleting a topic from a stream that does not exist.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    iggy::ffi::Client *client = GetLoggedInClient();

    ASSERT_THROW(client->delete_topic(make_string_identifier(stream_name), make_string_identifier(topic_name)),
                 std::exception);
}

TEST_F(LowLevelE2E_Topic, DeleteTopicOnNonExistentTopicThrows) {
    RecordProperty("description", "Throws when deleting a topic that does not exist.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    iggy::ffi::Client *client = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);

    ASSERT_THROW(client->delete_topic(make_string_identifier(stream_name), make_string_identifier(topic_name)),
                 std::exception);
}

TEST_F(LowLevelE2E_Topic, DeleteTopicTwiceThrows) {
    RecordProperty("description", "Throws when deleting the same topic a second time.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    iggy::ffi::Client *client = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));

    ASSERT_NO_THROW(client->delete_topic(make_string_identifier(stream_name), make_string_identifier(topic_name)));
    ASSERT_THROW(client->delete_topic(make_string_identifier(stream_name), make_string_identifier(topic_name)),
                 std::exception);
}

TEST_F(LowLevelE2E_Topic, DeleteTopicAfterStreamDeletionThrows) {
    RecordProperty("description", "Throws when deleting a topic after its stream has been deleted.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    iggy::ffi::Client *client = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->delete_stream(make_string_identifier(stream_name)));
    ForgetTrackedStream(stream_name);

    ASSERT_THROW(client->delete_topic(make_string_identifier(stream_name), make_string_identifier(topic_name)),
                 std::exception);
}

TEST_F(LowLevelE2E_Topic, DeleteTopicBeforeLoginThrows) {
    RecordProperty("description", "Rejects delete_topic before connect, and after connect but before login.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    iggy::ffi::Client *client = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));

    iggy::ffi::Client *unauthenticated_client = GetLoggedOutClient();

    ASSERT_THROW(
        unauthenticated_client->delete_topic(make_string_identifier(stream_name), make_string_identifier(topic_name)),
        std::exception);
    ASSERT_NO_THROW(unauthenticated_client->connect());
    ASSERT_THROW(
        unauthenticated_client->delete_topic(make_string_identifier(stream_name), make_string_identifier(topic_name)),
        std::exception);
}

TEST_F(LowLevelE2E_Topic, DeleteTopicWithInvalidStreamIdentifierThrows) {
    RecordProperty("description", "Rejects topic deletion requests that use invalid stream identifier formats.");
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
    ASSERT_THROW(client->delete_topic(std::move(invalid_kind_id), make_string_identifier(topic_name)), std::exception);

    iggy::ffi::Identifier invalid_numeric_id;
    invalid_numeric_id.kind   = "numeric";
    invalid_numeric_id.length = 1;
    invalid_numeric_id.value.push_back(1);
    ASSERT_THROW(client->delete_topic(std::move(invalid_numeric_id), make_string_identifier(topic_name)),
                 std::exception);
}

TEST_F(LowLevelE2E_Topic, DeleteTopicWithInvalidTopicIdentifierThrows) {
    RecordProperty("description", "Rejects topic deletion requests that use invalid topic identifier formats.");
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
    ASSERT_THROW(client->delete_topic(make_string_identifier(stream_name), std::move(invalid_kind_id)), std::exception);

    iggy::ffi::Identifier invalid_numeric_id;
    invalid_numeric_id.kind   = "numeric";
    invalid_numeric_id.length = 1;
    invalid_numeric_id.value.push_back(1);
    ASSERT_THROW(client->delete_topic(make_string_identifier(stream_name), std::move(invalid_numeric_id)),
                 std::exception);
}

TEST_F(LowLevelE2E_Topic, GetTopicReturnsTopicForExistingTopic) {
    RecordProperty("description", "Returns topic details for an existing topic.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);

    ASSERT_NO_THROW(
        client->create_topic(make_string_identifier(stream_name), topic_name, 3, "gzip", 1, "duration", 1000, "1GiB"));

    ASSERT_NO_THROW({
        const auto topic_details =
            client->get_topic(make_string_identifier(stream_name), make_string_identifier(topic_name));
        EXPECT_EQ(topic_details.name, topic_name);
        EXPECT_EQ(topic_details.partitions_count, 3u);
        EXPECT_EQ(topic_details.partitions.size(), 3u);
        EXPECT_EQ(topic_details.compression_algorithm, "gzip");
        EXPECT_EQ(topic_details.replication_factor, 1u);
        EXPECT_EQ(topic_details.message_expiry, 1000u);
        EXPECT_EQ(topic_details.max_topic_size, 1024ULL * 1024ULL * 1024ULL);
    });
}

TEST_F(LowLevelE2E_Topic, GetTopicBeforeLoginThrows) {
    RecordProperty("description", "Rejects get_topic before connect, and after connect but before login.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));

    iggy::ffi::Client *unauthenticated_client = GetLoggedOutClient();

    ASSERT_THROW(
        unauthenticated_client->get_topic(make_string_identifier(stream_name), make_string_identifier(topic_name)),
        std::exception);
    ASSERT_NO_THROW(unauthenticated_client->connect());
    ASSERT_THROW(
        unauthenticated_client->get_topic(make_string_identifier(stream_name), make_string_identifier(topic_name)),
        std::exception);
}

TEST_F(LowLevelE2E_Topic, GetTopicWithWrongStreamIdThrows) {
    RecordProperty("description", "Rejects get_topic when the topic belongs to a different stream.");
    const std::string first_stream_name  = GetRandomName();
    const std::string second_stream_name = GetRandomName();
    const std::string topic_name         = GetRandomName();
    iggy::ffi::Client *client            = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(first_stream_name));
    TrackStream(first_stream_name);
    ASSERT_NO_THROW(client->create_stream(second_stream_name));
    TrackStream(second_stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(first_stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));

    ASSERT_THROW(client->get_topic(make_string_identifier(second_stream_name), make_string_identifier(topic_name)),
                 std::exception);
}

TEST_F(LowLevelE2E_Topic, GetTopicWithWrongTopicThrows) {
    RecordProperty("description", "Rejects get_topic when the topic does not exist in the stream.");
    const std::string stream_name      = GetRandomName();
    const std::string topic_name       = GetRandomName();
    const std::string wrong_topic_name = GetRandomName();
    iggy::ffi::Client *client          = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));

    ASSERT_THROW(client->get_topic(make_string_identifier(stream_name), make_string_identifier(wrong_topic_name)),
                 std::exception);
}

TEST_F(LowLevelE2E_Topic, GetTopicAfterStreamDeletionThrows) {
    RecordProperty("description", "Rejects get_topic after the stream has been deleted.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->delete_stream(make_string_identifier(stream_name)));
    ForgetTrackedStream(stream_name);

    ASSERT_THROW(client->get_topic(make_string_identifier(stream_name), make_string_identifier(topic_name)),
                 std::exception);
}

TEST_F(LowLevelE2E_Topic, GetTopicAfterTopicDeletionThrows) {
    RecordProperty("description", "Rejects get_topic after the topic has been deleted.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->delete_topic(make_string_identifier(stream_name), make_string_identifier(topic_name)));

    ASSERT_THROW(client->get_topic(make_string_identifier(stream_name), make_string_identifier(topic_name)),
                 std::exception);
}

TEST_F(LowLevelE2E_Topic, GetTopicReturnsEmptyPartitionsForZeroPartitionTopic) {
    RecordProperty("description", "Returns an empty partitions vector for a topic created with zero partitions.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 0, "none", 0,
                                         "server_default", 0, "server_default"));

    ASSERT_NO_THROW({
        const auto topic_details =
            client->get_topic(make_string_identifier(stream_name), make_string_identifier(topic_name));
        EXPECT_EQ(topic_details.name, topic_name);
        EXPECT_EQ(topic_details.partitions_count, 0u);
        EXPECT_TRUE(topic_details.partitions.empty());
    });
}

TEST_F(LowLevelE2E_Topic, GetTopicReturnsMaxBoundaryPartitionCount) {
    RecordProperty("description", "Returns the maximum boundary partition count for a topic created with it.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1000, "none", 0,
                                         "server_default", 0, "server_default"));

    ASSERT_NO_THROW({
        const auto topic_details =
            client->get_topic(make_string_identifier(stream_name), make_string_identifier(topic_name));
        EXPECT_EQ(topic_details.name, topic_name);
        EXPECT_EQ(topic_details.partitions_count, 1000u);
        EXPECT_EQ(topic_details.partitions.size(), 1000u);
    });
}

TEST_F(LowLevelE2E_Topic, GetTopicIsStableAcrossBackToBackCalls) {
    RecordProperty("description", "Returns stable topic details across back-to-back get_topic calls.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(
        client->create_topic(make_string_identifier(stream_name), topic_name, 3, "gzip", 1, "duration", 1000, "1GiB"));

    iggy::ffi::TopicDetails first_topic{};
    iggy::ffi::TopicDetails second_topic{};
    ASSERT_NO_THROW({
        first_topic  = client->get_topic(make_string_identifier(stream_name), make_string_identifier(topic_name));
        second_topic = client->get_topic(make_string_identifier(stream_name), make_string_identifier(topic_name));
    });

    EXPECT_EQ(static_cast<std::string>(second_topic.name), static_cast<std::string>(first_topic.name));
    EXPECT_EQ(second_topic.message_expiry, first_topic.message_expiry);
    EXPECT_EQ(static_cast<std::string>(second_topic.compression_algorithm),
              static_cast<std::string>(first_topic.compression_algorithm));
    EXPECT_EQ(second_topic.max_topic_size, first_topic.max_topic_size);
    EXPECT_EQ(second_topic.replication_factor, first_topic.replication_factor);
    EXPECT_EQ(second_topic.partitions_count, first_topic.partitions_count);
    EXPECT_EQ(second_topic.partitions.size(), first_topic.partitions.size());
}

TEST_F(LowLevelE2E_Topic, GetTopicAgreesWithGetStreamTopicSummary) {
    RecordProperty("description", "Returns topic details that agree with get_stream topic summary fields.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(
        client->create_topic(make_string_identifier(stream_name), topic_name, 3, "gzip", 1, "duration", 1000, "1GiB"));

    ASSERT_NO_THROW({
        const auto stream_details = client->get_stream(make_string_identifier(stream_name));
        ASSERT_EQ(stream_details.topics.size(), 1u);

        const auto topic_summary = stream_details.topics.front();
        const auto topic_details =
            client->get_topic(make_string_identifier(stream_name), make_string_identifier(topic_name));

        EXPECT_EQ(static_cast<std::string>(topic_details.name), static_cast<std::string>(topic_summary.name));
        EXPECT_EQ(topic_details.message_expiry, topic_summary.message_expiry);
        EXPECT_EQ(static_cast<std::string>(topic_details.compression_algorithm),
                  static_cast<std::string>(topic_summary.compression_algorithm));
        EXPECT_EQ(topic_details.max_topic_size, topic_summary.max_topic_size);
        EXPECT_EQ(topic_details.replication_factor, topic_summary.replication_factor);
        EXPECT_EQ(topic_details.partitions_count, topic_summary.partitions_count);
        EXPECT_EQ(topic_details.partitions.size(), topic_summary.partitions_count);
    });
}

TEST_F(LowLevelE2E_Topic, GetTopicsReturnsCreatedTopicInputFields) {
    RecordProperty("description", "Creates topics in a stream, gets them, and verifies user-provided topic fields.");
    const std::string stream_name       = GetRandomName();
    const std::string first_topic_name  = GetRandomName();
    const std::string second_topic_name = GetRandomName();

    struct ExpectedTopic {
        std::uint32_t partitions_count;
        std::string compression_algorithm;
        std::uint64_t message_expiry;
        std::uint64_t max_topic_size;
        std::uint8_t replication_factor;
    };

    const std::unordered_map<std::string, ExpectedTopic> expected_topics = {
        {first_topic_name, {2, "gzip", 1000, 1024ULL * 1024ULL * 1024ULL, 1}},
        {second_topic_name,
         {0, "none", std::numeric_limits<std::uint64_t>::max(), std::numeric_limits<std::uint64_t>::max(), 1}},
    };

    iggy::ffi::Client *client = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), first_topic_name, 2, "gzip", 1,
                                         "duration", 1000, "1GiB"));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), second_topic_name, 0, "none", 1,
                                         "never_expire", 0, "unlimited"));

    ASSERT_NO_THROW({
        const auto topics = client->get_topics(make_string_identifier(stream_name));
        ASSERT_EQ(topics.size(), expected_topics.size());
        EXPECT_EQ(topics[0].name, first_topic_name);
        EXPECT_EQ(topics[1].name, second_topic_name);

        std::unordered_set<std::string> found_topic_names;
        for (const auto &topic : topics) {
            const std::string topic_name = static_cast<std::string>(topic.name);
            const auto expected          = expected_topics.find(topic_name);
            ASSERT_NE(expected, expected_topics.end()) << "Unexpected topic name returned: " << topic_name;

            EXPECT_EQ(topic.name, expected->first);
            EXPECT_EQ(topic.partitions_count, expected->second.partitions_count);
            EXPECT_EQ(topic.compression_algorithm, expected->second.compression_algorithm);
            EXPECT_EQ(topic.message_expiry, expected->second.message_expiry);
            EXPECT_EQ(topic.max_topic_size, expected->second.max_topic_size);
            EXPECT_EQ(topic.replication_factor, expected->second.replication_factor);
            found_topic_names.insert(topic_name);
        }
        EXPECT_EQ(found_topic_names.size(), expected_topics.size());
    });
}

TEST_F(LowLevelE2E_Topic, GetTopicsBeforeLoginThrows) {
    RecordProperty("description", "Rejects get_topics before connect, and after connect but before login.");
    const std::string stream_name = GetRandomName();

    iggy::ffi::Client *setup_client = GetLoggedInClient();
    ASSERT_NO_THROW(setup_client->create_stream(stream_name));
    TrackStream(stream_name);

    iggy::ffi::Client *unauthenticated_client = GetLoggedOutClient();

    ASSERT_THROW(unauthenticated_client->get_topics(make_string_identifier(stream_name)), std::exception);
    ASSERT_NO_THROW(unauthenticated_client->connect());
    ASSERT_THROW(unauthenticated_client->get_topics(make_string_identifier(stream_name)), std::exception);
}

TEST_F(LowLevelE2E_Topic, GetTopicsReturnsEmptyForStreamWithoutTopics) {
    RecordProperty("description", "Returns an empty topic list for an existing stream that has no topics.");
    const std::string stream_name = GetRandomName();

    iggy::ffi::Client *client = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);

    ASSERT_NO_THROW({
        const auto topics = client->get_topics(make_string_identifier(stream_name));
        EXPECT_TRUE(topics.empty());
    });
}

TEST_F(LowLevelE2E_Topic, GetTopicsAfterTopicDeletionReturnsRemainingTopics) {
    RecordProperty("description", "Returns only non-deleted topics after a topic is deleted from the stream.");
    const std::string stream_name     = GetRandomName();
    const std::string deleted_topic   = GetRandomName();
    const std::string remaining_topic = GetRandomName();

    iggy::ffi::Client *client = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), deleted_topic, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), remaining_topic, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->delete_topic(make_string_identifier(stream_name), make_string_identifier(deleted_topic)));

    ASSERT_NO_THROW({
        const auto topics = client->get_topics(make_string_identifier(stream_name));
        ASSERT_EQ(topics.size(), 1u);
        EXPECT_EQ(topics.front().name, remaining_topic);
    });
}

TEST_F(LowLevelE2E_Topic, GetTopicsAfterTopicUpdateReturnsUpdatedInputFields) {
    RecordProperty("description", "Returns updated user-provided topic fields after a topic update.");
    const std::string stream_name        = GetRandomName();
    const std::string original_topic     = GetRandomName();
    const std::string updated_topic_name = GetRandomName();

    iggy::ffi::Client *client = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), original_topic, 2, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->update_topic(make_string_identifier(stream_name), make_string_identifier(original_topic),
                                         updated_topic_name, "gzip", 1, "duration", 1000, "1GiB"));

    ASSERT_NO_THROW({
        const auto topics = client->get_topics(make_string_identifier(stream_name));
        ASSERT_EQ(topics.size(), 1u);
        EXPECT_EQ(topics.front().name, updated_topic_name);
        EXPECT_EQ(topics.front().partitions_count, 2u);
        EXPECT_EQ(topics.front().compression_algorithm, "gzip");
        EXPECT_EQ(topics.front().replication_factor, 1u);
        EXPECT_EQ(topics.front().message_expiry, 1000u);
        EXPECT_EQ(topics.front().max_topic_size, 1024ULL * 1024ULL * 1024ULL);
    });
}

TEST_F(LowLevelE2E_Topic, UpdateTopicWorksCorrectly) {
    RecordProperty("description", "Returns a topic summary that matches topic details after updating a topic.");
    const std::string stream_name        = GetRandomName();
    const std::string original_topic     = GetRandomName();
    const std::string updated_topic_name = GetRandomName();

    iggy::ffi::Client *client = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), original_topic, 2, "none", 1,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->update_topic(make_string_identifier(stream_name), make_string_identifier(original_topic),
                                         updated_topic_name, "gzip", 27, "duration", 1000, "1GiB"));

    ASSERT_NO_THROW({
        const auto topic_details =
            client->get_topic(make_string_identifier(stream_name), make_string_identifier(updated_topic_name));
        const auto topics = client->get_topics(make_string_identifier(stream_name));
        ASSERT_EQ(topics.size(), 1u);

        const auto &topic_summary = topics.front();
        EXPECT_EQ(topic_summary.id, topic_details.id);
        EXPECT_EQ(topic_summary.created_at, topic_details.created_at);
        EXPECT_EQ(topic_summary.name, topic_details.name);
        EXPECT_EQ(topic_summary.size_bytes, topic_details.size_bytes);
        EXPECT_EQ(topic_summary.message_expiry, topic_details.message_expiry);
        EXPECT_EQ(topic_summary.compression_algorithm, topic_details.compression_algorithm);
        EXPECT_EQ(topic_summary.max_topic_size, topic_details.max_topic_size);
        EXPECT_EQ(topic_summary.replication_factor, topic_details.replication_factor);
        EXPECT_EQ(topic_summary.messages_count, topic_details.messages_count);
        EXPECT_EQ(topic_summary.partitions_count, topic_details.partitions_count);
    });
}

TEST_F(LowLevelE2E_Topic, UpdateTopicDoesNotChangePartitionsCount) {
    RecordProperty("description", "Preserves the topic partition count after updating topic metadata.");
    const std::string stream_name            = GetRandomName();
    const std::string original_topic         = GetRandomName();
    const std::string updated_topic_name     = GetRandomName();
    constexpr std::uint32_t partitions_count = 3;

    iggy::ffi::Client *client = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), original_topic, partitions_count, "none",
                                         1, "server_default", 0, "server_default"));

    ASSERT_NO_THROW(client->update_topic(make_string_identifier(stream_name), make_string_identifier(original_topic),
                                         updated_topic_name, "gzip", 27, "duration", 1000, "1GiB"));

    ASSERT_NO_THROW({
        const auto topic_details =
            client->get_topic(make_string_identifier(stream_name), make_string_identifier(updated_topic_name));
        EXPECT_EQ(topic_details.partitions_count, partitions_count);
        EXPECT_EQ(topic_details.partitions.size(), partitions_count);
    });
}

TEST_F(LowLevelE2E_Topic, UpdateTopicDoesNotChangeMessages) {
    RecordProperty("description", "Keeps existing messages readable after updating topic metadata.");
    const std::string stream_name        = GetRandomName();
    const std::string original_topic     = GetRandomName();
    const std::string updated_topic_name = GetRandomName();

    iggy::ffi::Client *client = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), original_topic, 1, "none", 1,
                                         "server_default", 0, "server_default"));

    const auto created_stream = client->get_stream(make_string_identifier(stream_name));
    ASSERT_EQ(created_stream.topics.size(), 1u);
    const auto topic_id = created_stream.topics.front().id;

    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    messages.push_back(iggy::ffi::make_message(to_payload("message-before-topic-update")));
    ASSERT_NO_THROW(client->send_messages(make_numeric_identifier(created_stream.id), make_numeric_identifier(topic_id),
                                          "partition_id", partition_id_bytes(0), std::move(messages)));

    ASSERT_NO_THROW(client->update_topic(make_string_identifier(stream_name), make_string_identifier(original_topic),
                                         updated_topic_name, "gzip", 27, "duration", 1000, "1GiB"));

    ASSERT_NO_THROW({
        const auto polled = client->poll_messages(make_numeric_identifier(created_stream.id),
                                                  make_string_identifier(updated_topic_name), 0, "consumer",
                                                  make_numeric_identifier(1), "offset", 0, 10, false);
        ASSERT_EQ(polled.count, 1u);
        ASSERT_EQ(polled.messages.size(), 1u);
        const std::string actual(polled.messages[0].payload.begin(), polled.messages[0].payload.end());
        EXPECT_EQ(actual, "message-before-topic-update");
    });
}

TEST_F(LowLevelE2E_Topic, UpdateTopicWithAllOptionCombinationsUpdatesInputFields) {
    RecordProperty("description",
                   "Updates a topic across supported option combinations and verifies deterministic updated fields.");
    const std::string stream_name = GetRandomName();
    std::string topic_name        = GetRandomName();

    const std::vector<std::string> compression_algorithms = {"none", "gzip"};
    const std::vector<std::uint8_t> replication_factors   = {0, 1, 27};
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

    iggy::ffi::Client *client = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 2, "none", 1,
                                         "server_default", 0, "server_default"));

    for (const auto &compression_algorithm : compression_algorithms) {
        for (const auto replication_factor : replication_factors) {
            for (const auto &expiry_option : expiry_options) {
                for (const auto &max_topic_size : max_topic_sizes) {
                    const std::string updated_topic_name = GetRandomName();
                    SCOPED_TRACE(
                        "compression=" + compression_algorithm + ", replication=" + std::to_string(replication_factor) +
                        ", expiry_kind=" + expiry_option.kind +
                        ", expiry_value=" + std::to_string(expiry_option.value) + ", max_topic_size=" + max_topic_size);

                    ASSERT_NO_THROW(client->update_topic(make_string_identifier(stream_name),
                                                         make_string_identifier(topic_name), updated_topic_name,
                                                         compression_algorithm, replication_factor, expiry_option.kind,
                                                         expiry_option.value, max_topic_size));
                    topic_name = updated_topic_name;
                }
            }
        }
    }
}

TEST_F(LowLevelE2E_Topic, UpdateTopicWithSameOptionsIsIdempotent) {
    RecordProperty("description", "Calling update_topic twice with the same options returns the same topic details.");
    const std::string stream_name        = GetRandomName();
    const std::string original_topic     = GetRandomName();
    const std::string updated_topic_name = GetRandomName();

    iggy::ffi::Client *client = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), original_topic, 2, "none", 1,
                                         "server_default", 0, "server_default"));

    const auto created_topic =
        client->get_topic(make_string_identifier(stream_name), make_string_identifier(original_topic));

    ASSERT_NO_THROW(client->update_topic(make_string_identifier(stream_name), make_numeric_identifier(created_topic.id),
                                         updated_topic_name, "gzip", 1, "duration", 1000, "1GiB"));
    const auto first_update =
        client->get_topic(make_string_identifier(stream_name), make_numeric_identifier(created_topic.id));

    ASSERT_NO_THROW(client->update_topic(make_string_identifier(stream_name), make_numeric_identifier(created_topic.id),
                                         updated_topic_name, "gzip", 1, "duration", 1000, "1GiB"));
    const auto second_update =
        client->get_topic(make_string_identifier(stream_name), make_numeric_identifier(created_topic.id));

    EXPECT_EQ(second_update.id, first_update.id);
    EXPECT_EQ(second_update.name, first_update.name);
    EXPECT_EQ(second_update.partitions_count, first_update.partitions_count);
    EXPECT_EQ(second_update.compression_algorithm, first_update.compression_algorithm);
    EXPECT_EQ(second_update.replication_factor, first_update.replication_factor);
    EXPECT_EQ(second_update.message_expiry, first_update.message_expiry);
    EXPECT_EQ(second_update.max_topic_size, first_update.max_topic_size);
}

TEST_F(LowLevelE2E_Topic, UpdateTopicWithDuplicateTopicNameThrows) {
    RecordProperty("description", "Rejects renaming a topic to another topic's existing name.");
    const std::string stream_name       = GetRandomName();
    const std::string first_topic_name  = GetRandomName();
    const std::string second_topic_name = GetRandomName();

    iggy::ffi::Client *client = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), first_topic_name, 1, "none", 1,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), second_topic_name, 1, "none", 1,
                                         "server_default", 0, "server_default"));

    ASSERT_THROW(client->update_topic(make_string_identifier(stream_name), make_string_identifier(first_topic_name),
                                      second_topic_name, "gzip", 1, "duration", 1000, "1GiB"),
                 std::exception);
}

TEST_F(LowLevelE2E_Topic, UpdateTopicWithInvalidNamesThrows) {
    RecordProperty("description", "Rejects invalid topic names when updating a topic.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    iggy::ffi::Client *client = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 1,
                                         "server_default", 0, "server_default"));

    const std::vector<std::string> invalid_topic_names = {
        "",
        std::string(256, 'b'),
    };

    for (const auto &invalid_topic_name : invalid_topic_names) {
        SCOPED_TRACE("invalid_topic_name_length=" + std::to_string(invalid_topic_name.size()));

        ASSERT_THROW(client->update_topic(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                          invalid_topic_name, "gzip", 1, "duration", 1000, "1GiB"),
                     std::exception);
    }
}

TEST_F(LowLevelE2E_Topic, UpdateTopicFailedValidationDoesNotMutateTopic) {
    RecordProperty("description", "Keeps the topic unchanged when update_topic fails wrapper validation.");
    const std::string stream_name        = GetRandomName();
    const std::string topic_name         = GetRandomName();
    const std::string updated_topic_name = GetRandomName();

    iggy::ffi::Client *client = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(
        client->create_topic(make_string_identifier(stream_name), topic_name, 2, "gzip", 1, "duration", 1000, "1GiB"));

    const auto topic_before_update =
        client->get_topic(make_string_identifier(stream_name), make_string_identifier(topic_name));

    ASSERT_THROW(client->update_topic(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                      updated_topic_name, "none", 1, "duration", 2000, "not-a-size"),
                 std::exception);

    const auto topic_after_failed_update =
        client->get_topic(make_string_identifier(stream_name), make_string_identifier(topic_name));

    EXPECT_EQ(topic_after_failed_update.id, topic_before_update.id);
    EXPECT_EQ(topic_after_failed_update.name, topic_before_update.name);
    EXPECT_EQ(topic_after_failed_update.partitions_count, topic_before_update.partitions_count);
    EXPECT_EQ(topic_after_failed_update.compression_algorithm, topic_before_update.compression_algorithm);
    EXPECT_EQ(topic_after_failed_update.replication_factor, topic_before_update.replication_factor);
    EXPECT_EQ(topic_after_failed_update.message_expiry, topic_before_update.message_expiry);
    EXPECT_EQ(topic_after_failed_update.max_topic_size, topic_before_update.max_topic_size);

    EXPECT_THROW(client->get_topic(make_string_identifier(stream_name), make_string_identifier(updated_topic_name)),
                 std::exception);
}

TEST_F(LowLevelE2E_Topic, UpdateTopicBeforeLoginThrows) {
    RecordProperty("description", "Rejects update_topic before connect, and after connect but before login.");
    const std::string stream_name        = GetRandomName();
    const std::string topic_name         = GetRandomName();
    const std::string updated_topic_name = GetRandomName();

    iggy::ffi::Client *client = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 1,
                                         "server_default", 0, "server_default"));

    iggy::ffi::Client *unauthenticated_client = GetLoggedOutClient();

    ASSERT_THROW(
        unauthenticated_client->update_topic(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                             updated_topic_name, "gzip", 1, "duration", 1000, "1GiB"),
        std::exception);
    ASSERT_NO_THROW(unauthenticated_client->connect());
    ASSERT_THROW(
        unauthenticated_client->update_topic(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                             updated_topic_name, "gzip", 1, "duration", 1000, "1GiB"),
        std::exception);
}

TEST_F(LowLevelE2E_Topic, UpdateTopicOnNonExistentStreamThrows) {
    RecordProperty("description", "Throws when updating a topic on a stream that does not exist.");
    const std::string stream_name        = GetRandomName();
    const std::string topic_name         = GetRandomName();
    const std::string updated_topic_name = GetRandomName();

    iggy::ffi::Client *client = GetLoggedInClient();

    ASSERT_THROW(client->update_topic(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                      updated_topic_name, "gzip", 1, "duration", 1000, "1GiB"),
                 std::exception);
}

TEST_F(LowLevelE2E_Topic, UpdateTopicOnNonExistentTopicThrows) {
    RecordProperty("description", "Throws when updating a topic that does not exist.");
    const std::string stream_name        = GetRandomName();
    const std::string topic_name         = GetRandomName();
    const std::string updated_topic_name = GetRandomName();

    iggy::ffi::Client *client = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);

    ASSERT_THROW(client->update_topic(make_string_identifier(stream_name), make_string_identifier(topic_name),
                                      updated_topic_name, "gzip", 1, "duration", 1000, "1GiB"),
                 std::exception);
}

TEST_F(LowLevelE2E_Topic, GetTopicsAfterStreamDeletionReturnsEmpty) {
    RecordProperty("description", "Returns an empty topic list after deleting the stream that owned the topic.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    iggy::ffi::Client *client = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->delete_stream(make_string_identifier(stream_name)));
    ForgetTrackedStream(stream_name);

    ASSERT_NO_THROW({
        const auto topics = client->get_topics(make_string_identifier(stream_name));
        EXPECT_TRUE(topics.empty());
    });
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
