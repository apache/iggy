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

// TODO(slbotbm): create fixture for setup/teardown.x

#include <cstdint>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "lib.rs.h"
#include "tests/common/test_helpers.hpp"

// TODO(slbotbm): Add tests for purge_topic after implementing send_messages(...).

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
