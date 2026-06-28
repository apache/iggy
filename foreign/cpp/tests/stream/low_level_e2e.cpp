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
#include <string>
#include <unordered_map>
#include <vector>

#include <gtest/gtest.h>

#include "lib.rs.h"
#include "tests/common/test_helpers.hpp"

class LowLevelE2E_Stream : public E2ETestFixture {};

TEST_F(LowLevelE2E_Stream, CreateStreamAfterLogin) {
    RecordProperty("description", "Creates a stream successfully after authenticating.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();
    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
}

TEST_F(LowLevelE2E_Stream, CreateDuplicateStreamThrows) {
    RecordProperty("description", "Rejects creating the same stream twice.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();
    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_THROW(client->create_stream(stream_name), std::exception);
}

TEST_F(LowLevelE2E_Stream, CreateStreamBeforeLoginThrows) {
    RecordProperty("description", "Throws when stream creation is attempted before authentication.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedOutClient();

    ASSERT_THROW(client->create_stream(stream_name), std::exception);
    ASSERT_NO_THROW(client->connect());
    ASSERT_THROW(client->create_stream(stream_name), std::exception);
}

TEST_F(LowLevelE2E_Stream, CreateStreamValidatesNameConstraintsAndUniqueness) {
    RecordProperty("description",
                   "Validates stream name length constraints and accepts the maximum allowed name length.");
    iggy::ffi::Client *client = GetLoggedInClient();

    const std::string illegal_stream_names[] = {
        "",
        std::string(256, 'b'),
    };
    for (const auto &stream_name : illegal_stream_names) {
        SCOPED_TRACE(stream_name);
        ASSERT_THROW(client->create_stream(stream_name), std::exception);
    }

    const std::string max_length_name(255, 'a');
    ASSERT_NO_THROW(client->create_stream(max_length_name));
    TrackStream(max_length_name);
}

TEST_F(LowLevelE2E_Stream, CreateStreamWithEmojiName) {
    RecordProperty("description", "Creates a stream with a UTF-8 emoji name.");
    const std::string stream_name = "🚀🚀🚀🚀Apache Iggy🚀🚀🚀🚀";
    iggy::ffi::Client *client     = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW({
        const auto stream_details              = client->get_stream(make_string_identifier(stream_name));
        const std::string returned_stream_name = static_cast<std::string>(stream_details.name);
        EXPECT_EQ(returned_stream_name, stream_name);
        EXPECT_EQ(stream_details.topics_count, 0u);
        EXPECT_EQ(stream_details.topics.size(), 0u);
    });
}

TEST_F(LowLevelE2E_Stream, StreamCreatedAndDeletedSuccessfully) {
    RecordProperty("description", "Creates a stream and deletes it successfully by string identifier.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();
    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);

    ASSERT_NO_THROW(client->delete_stream(make_string_identifier(stream_name)));
    ForgetTrackedStream(stream_name);
}

TEST_F(LowLevelE2E_Stream, DeleteNotCreatedStreamThrows) {
    RecordProperty("description", "Throws when deleting a stream that does not exist.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    ASSERT_THROW(client->delete_stream(make_string_identifier(stream_name)), std::exception);
}

TEST_F(LowLevelE2E_Stream, DeleteStreamBeforeLoginThrows) {
    RecordProperty("description", "Throws when stream deletion is attempted before authentication.");
    const std::string stream_name = GetRandomName();

    iggy::ffi::Client *client = GetLoggedOutClient();

    ASSERT_THROW(client->delete_stream(make_string_identifier(stream_name)), std::exception);

    ASSERT_NO_THROW(client->connect());

    ASSERT_THROW(client->delete_stream(make_string_identifier(stream_name)), std::exception);
}

TEST_F(LowLevelE2E_Stream, DeleteStreamTwiceThrows) {
    RecordProperty("description", "Throws when deleting the same stream a second time.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();
    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);

    ASSERT_NO_THROW(client->delete_stream(make_string_identifier(stream_name)));
    ForgetTrackedStream(stream_name);
    ASSERT_THROW(client->delete_stream(make_string_identifier(stream_name)), std::exception);
}

TEST_F(LowLevelE2E_Stream, DeleteStreamWithInvalidIdentifierThrows) {
    RecordProperty("description", "Rejects stream deletion requests that use invalid identifier formats.");
    iggy::ffi::Client *client = GetLoggedInClient();

    iggy::ffi::Identifier invalid_kind_id;
    invalid_kind_id.kind   = "invalid";
    invalid_kind_id.length = 4;
    invalid_kind_id.value  = {1, 0, 0, 0};
    ASSERT_THROW(client->delete_stream(std::move(invalid_kind_id)), std::exception);

    iggy::ffi::Identifier invalid_numeric_id;
    invalid_numeric_id.kind   = "numeric";
    invalid_numeric_id.length = 1;
    invalid_numeric_id.value.push_back(1);
    ASSERT_THROW(client->delete_stream(std::move(invalid_numeric_id)), std::exception);
}

TEST_F(LowLevelE2E_Stream, GetStreamDetailsWithInvalidIdentifierThrows) {
    RecordProperty("description", "Rejects stream detail lookups that use invalid identifier formats.");
    iggy::ffi::Client *client = GetLoggedInClient();

    iggy::ffi::Identifier invalid_kind_id;
    invalid_kind_id.kind   = "invalid";
    invalid_kind_id.length = 4;
    invalid_kind_id.value  = {1, 0, 0, 0};
    ASSERT_THROW(client->get_stream(std::move(invalid_kind_id)), std::exception);

    iggy::ffi::Identifier invalid_numeric_id;
    invalid_numeric_id.kind   = "numeric";
    invalid_numeric_id.length = 1;
    invalid_numeric_id.value.push_back(1);
    ASSERT_THROW(client->get_stream(std::move(invalid_numeric_id)), std::exception);
}

TEST_F(LowLevelE2E_Stream, GetStreamByStringIdentifierReturnsStreamDetails) {
    RecordProperty("description", "Returns expected stream details when looked up by string identifier.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();
    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);

    ASSERT_NO_THROW({
        const auto stream_details = client->get_stream(make_string_identifier(stream_name));
        EXPECT_EQ(stream_details.name, stream_name);
        EXPECT_EQ(stream_details.topics_count, 0u);
        EXPECT_EQ(stream_details.topics.size(), 0u);
        EXPECT_EQ(stream_details.messages_count, 0u);
        EXPECT_EQ(stream_details.size_bytes, 0u);
    });
}

TEST_F(LowLevelE2E_Stream, GetNonExistentStreamDetailsThrows) {
    RecordProperty("description", "Throws when requesting details for a stream that does not exist.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();
    ASSERT_THROW(client->get_stream(make_string_identifier(stream_name)), std::exception);
}

TEST_F(LowLevelE2E_Stream, GetStreamDetailsBeforeLoginThrows) {
    RecordProperty("description", "Throws when stream details are requested before authentication.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedOutClient();

    ASSERT_THROW(client->get_stream(make_string_identifier(stream_name)), std::exception);
    ASSERT_NO_THROW(client->connect());
    ASSERT_THROW(client->get_stream(make_string_identifier(stream_name)), std::exception);
}

TEST_F(LowLevelE2E_Stream, GetDeletedStreamDetailsThrows) {
    RecordProperty("description", "Throws when requesting details for a stream after it has been deleted.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();
    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->get_stream(make_string_identifier(stream_name)));
    ASSERT_NO_THROW(client->delete_stream(make_string_identifier(stream_name)));
    ForgetTrackedStream(stream_name);
    ASSERT_THROW(client->get_stream(make_string_identifier(stream_name)), std::exception);
}

TEST_F(LowLevelE2E_Stream, GetStreamByNumericIdentifierReturnsStreamDetails) {
    RecordProperty("description", "Returns expected stream details when looked up by numeric identifier.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();
    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);

    std::uint32_t stream_id = 0;
    ASSERT_NO_THROW({
        const auto by_string = client->get_stream(make_string_identifier(stream_name));
        stream_id            = by_string.id;
    });

    ASSERT_NO_THROW({
        const auto by_numeric = client->get_stream(make_numeric_identifier(stream_id));
        EXPECT_EQ(by_numeric.id, stream_id);
        EXPECT_EQ(by_numeric.name, stream_name);
        EXPECT_EQ(by_numeric.topics_count, 0u);
        EXPECT_EQ(by_numeric.topics.size(), 0u);
    });
}

TEST_F(LowLevelE2E_Stream, GetStreamsReturnsEmptyAfterCleanup) {
    RecordProperty("description", "Verifies get_streams returns empty vector after cleaning up all streams.");
    iggy::ffi::Client *client = GetLoggedInClient();

    auto streams = client->get_streams();
    for (const auto &s : streams) {
        client->delete_stream(make_numeric_identifier(s.id));
    }

    streams = client->get_streams();
    ASSERT_EQ(streams.size(), 0u);
}

TEST_F(LowLevelE2E_Stream, GetStreamsReturnsStreamAfterCreation) {
    RecordProperty("description", "Verifies created stream appears in get_streams result.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    client->create_stream(stream_name);
    TrackStream(stream_name);
    auto streams = client->get_streams();
    ASSERT_GE(streams.size(), 1u);

    bool found = false;
    for (const auto &s : streams) {
        if (std::string(s.name) == stream_name) {
            found = true;
            EXPECT_GT(s.created_at, static_cast<std::uint64_t>(0));
            EXPECT_EQ(s.size_bytes, static_cast<std::uint64_t>(0));
            EXPECT_EQ(s.messages_count, static_cast<std::uint64_t>(0));
            EXPECT_EQ(s.topics_count, 0u);
            break;
        }
    }
    ASSERT_TRUE(found) << "Stream '" << stream_name << "' not found in get_streams result";
}

TEST_F(LowLevelE2E_Stream, GetStreamsFieldsVerification) {
    RecordProperty("description",
                   "Verifies get_streams returns correct field values after creating stream with topic and messages.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    client->create_stream(stream_name);
    TrackStream(stream_name);
    auto stream                  = client->get_stream(make_string_identifier(stream_name));
    const std::string topic_name = GetRandomName();
    client->create_topic(make_numeric_identifier(stream.id), topic_name, 1, "none", 0, "never_expire", 0,
                         "server_default");

    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    for (std::uint32_t i = 0; i < 5; i++) {
        auto msg = iggy::ffi::make_message(to_payload("field-verify-message-" + std::to_string(i)));
        messages.push_back(std::move(msg));
    }
    client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), "partition_id",
                          partition_id_bytes(0), std::move(messages));

    auto streams = client->get_streams();
    ASSERT_GE(streams.size(), 1u);

    bool found = false;
    for (const auto &s : streams) {
        if (std::string(s.name) == stream_name) {
            found = true;
            EXPECT_EQ(s.topics_count, 1u);
            EXPECT_EQ(s.messages_count, 5u);
            break;
        }
    }
    ASSERT_TRUE(found) << "Stream '" << stream_name << "' not found in get_streams result";
}

TEST_F(LowLevelE2E_Stream, GetStreamsBeforeLoginThrows) {
    RecordProperty("description", "Throws when get_streams is called before authentication.");
    iggy::ffi::Client *client = GetLoggedOutClient();

    ASSERT_THROW(client->get_streams(), std::exception);
    ASSERT_NO_THROW(client->connect());
    ASSERT_THROW(client->get_streams(), std::exception);
}

TEST_F(LowLevelE2E_Stream, GetStreamsConsistentWithGetStream) {
    RecordProperty("description", "Verifies get_streams result is consistent with get_stream for the same stream.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    client->create_stream(stream_name);
    TrackStream(stream_name);

    std::string list_name;
    std::uint32_t list_id           = 0;
    std::uint32_t list_topics_count = 0;
    std::uint64_t list_created_at   = 0;
    std::uint64_t list_size_bytes   = 0;
    auto streams                    = client->get_streams();
    for (const auto &s : streams) {
        if (std::string(s.name) == stream_name) {
            list_name         = std::string(s.name);
            list_id           = s.id;
            list_topics_count = s.topics_count;
            list_created_at   = s.created_at;
            list_size_bytes   = s.size_bytes;
            break;
        }
    }
    ASSERT_FALSE(list_name.empty()) << "Stream '" << stream_name << "' not found in get_streams result";

    auto single        = client->get_stream(make_string_identifier(stream_name));
    auto single_name   = std::string(single.name);
    auto single_topics = single.topics_count;

    EXPECT_EQ(list_name, single_name);
    EXPECT_EQ(list_id, single.id);
    EXPECT_EQ(list_topics_count, single_topics);
    EXPECT_EQ(list_created_at, single.created_at);
    EXPECT_EQ(list_size_bytes, single.size_bytes);
}

TEST_F(LowLevelE2E_Stream, GetStreamsRepeatedCallsReturnSameResult) {
    RecordProperty("description", "Verifies repeated get_streams calls return consistent results.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    client->create_stream(stream_name);
    TrackStream(stream_name);

    auto streams1 = client->get_streams();
    auto streams2 = client->get_streams();
    auto streams3 = client->get_streams();

    ASSERT_EQ(streams1.size(), streams2.size());
    ASSERT_EQ(streams2.size(), streams3.size());

    auto contains_stream = [&](const rust::Vec<iggy::ffi::Stream> &vec) {
        for (const auto &s : vec) {
            if (std::string(s.name) == stream_name) {
                return true;
            }
        }
        return false;
    };

    ASSERT_TRUE(contains_stream(streams1)) << "Stream not found in first call";
    ASSERT_TRUE(contains_stream(streams2)) << "Stream not found in second call";
    ASSERT_TRUE(contains_stream(streams3)) << "Stream not found in third call";
}

TEST_F(LowLevelE2E_Stream, PurgeStreamOnNonExistentStreamThrows) {
    RecordProperty("description", "Throws when purging a stream that does not exist.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    ASSERT_THROW(client->purge_stream(make_string_identifier(stream_name)), std::exception);
}

TEST_F(LowLevelE2E_Stream, PurgeStreamAfterStreamDeletionThrows) {
    RecordProperty("description", "Throws when purging a stream after it has been deleted.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->delete_stream(make_string_identifier(stream_name)));
    ForgetTrackedStream(stream_name);

    ASSERT_THROW(client->purge_stream(make_string_identifier(stream_name)), std::exception);
}

TEST_F(LowLevelE2E_Stream, PurgeStreamWithInvalidIdentifierThrows) {
    RecordProperty("description", "Rejects stream purge requests that use invalid identifier formats.");
    iggy::ffi::Client *client = GetLoggedInClient();

    iggy::ffi::Identifier invalid_kind_id;
    invalid_kind_id.kind   = "invalid";
    invalid_kind_id.length = 4;
    invalid_kind_id.value  = {1, 0, 0, 0};
    ASSERT_THROW(client->purge_stream(std::move(invalid_kind_id)), std::exception);

    iggy::ffi::Identifier invalid_numeric_id;
    invalid_numeric_id.kind   = "numeric";
    invalid_numeric_id.length = 1;
    invalid_numeric_id.value.push_back(1);
    ASSERT_THROW(client->purge_stream(std::move(invalid_numeric_id)), std::exception);
}

TEST_F(LowLevelE2E_Stream, PurgeStreamPreservesStreamMetadata) {
    RecordProperty("description", "Preserves stream identity and topic metadata after purging stream messages.");
    const std::string stream_name       = GetRandomName();
    const std::string first_topic_name  = GetRandomName();
    const std::string second_topic_name = GetRandomName();
    iggy::ffi::Client *client           = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), first_topic_name, 2, "gzip", 1,
                                         "duration", 1000, "1GiB"));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), second_topic_name, 3, "none", 0,
                                         "never_expire", 0, "server_default"));

    const auto stream_before_purge = client->get_stream(make_string_identifier(stream_name));
    ASSERT_EQ(stream_before_purge.topics.size(), 2u);

    rust::Vec<iggy::ffi::IggyMessageToSend> first_topic_messages;
    first_topic_messages.push_back(iggy::ffi::make_message(to_payload("preserve-stream-metadata")));
    ASSERT_NO_THROW(client->send_messages(make_numeric_identifier(stream_before_purge.id),
                                          make_string_identifier(first_topic_name), "partition_id",
                                          partition_id_bytes(0), std::move(first_topic_messages)));

    const auto stream_with_messages = client->get_stream(make_string_identifier(stream_name));
    EXPECT_GT(stream_with_messages.messages_count, 0u);
    EXPECT_GT(stream_with_messages.size_bytes, 0u);

    struct TopicMetadata {
        std::uint32_t id;
        std::uint64_t created_at;
        std::string name;
        std::uint64_t message_expiry;
        std::string compression_algorithm;
        std::uint64_t max_topic_size;
        std::uint8_t replication_factor;
        std::uint32_t partitions_count;
    };
    std::unordered_map<std::string, TopicMetadata> topics_before_purge;
    for (const auto &topic : stream_with_messages.topics) {
        topics_before_purge[static_cast<std::string>(topic.name)] = {
            topic.id,
            topic.created_at,
            static_cast<std::string>(topic.name),
            topic.message_expiry,
            static_cast<std::string>(topic.compression_algorithm),
            topic.max_topic_size,
            topic.replication_factor,
            topic.partitions_count};
    }

    ASSERT_NO_THROW(client->purge_stream(make_string_identifier(stream_name)));

    const auto stream_after_purge = client->get_stream(make_string_identifier(stream_name));
    EXPECT_EQ(stream_after_purge.id, stream_with_messages.id);
    EXPECT_EQ(stream_after_purge.created_at, stream_with_messages.created_at);
    EXPECT_EQ(stream_after_purge.name, stream_with_messages.name);
    EXPECT_EQ(stream_after_purge.topics_count, stream_with_messages.topics_count);
    ASSERT_EQ(stream_after_purge.topics.size(), stream_with_messages.topics.size());

    for (const auto &topic : stream_after_purge.topics) {
        const std::string topic_name = static_cast<std::string>(topic.name);
        const auto metadata_it       = topics_before_purge.find(topic_name);
        ASSERT_NE(metadata_it, topics_before_purge.end());
        const auto &metadata = metadata_it->second;
        EXPECT_EQ(topic.id, metadata.id);
        EXPECT_EQ(topic.created_at, metadata.created_at);
        EXPECT_EQ(topic.name, metadata.name);
        EXPECT_EQ(topic.message_expiry, metadata.message_expiry);
        EXPECT_EQ(topic.compression_algorithm, metadata.compression_algorithm);
        EXPECT_EQ(topic.max_topic_size, metadata.max_topic_size);
        EXPECT_EQ(topic.replication_factor, metadata.replication_factor);
        EXPECT_EQ(topic.partitions_count, metadata.partitions_count);
    }
}

TEST_F(LowLevelE2E_Stream, PurgeStreamRemovesMessagesAndPreservesTopics) {
    RecordProperty("description", "Purges all stream messages while keeping the stream and topics intact.");
    const std::string stream_name       = GetRandomName();
    const std::string first_topic_name  = GetRandomName();
    const std::string second_topic_name = GetRandomName();
    iggy::ffi::Client *client           = GetLoggedInClient();

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
        first_topic_messages.push_back(iggy::ffi::make_message(to_payload("purge-stream-first-" + std::to_string(i))));
    }
    ASSERT_NO_THROW(client->send_messages(make_numeric_identifier(created_stream.id),
                                          make_numeric_identifier(first_topic_id), "partition_id",
                                          partition_id_bytes(0), std::move(first_topic_messages)));

    rust::Vec<iggy::ffi::IggyMessageToSend> second_topic_messages;
    for (std::uint32_t i = 0; i < 2; ++i) {
        second_topic_messages.push_back(
            iggy::ffi::make_message(to_payload("purge-stream-second-" + std::to_string(i))));
    }
    ASSERT_NO_THROW(client->send_messages(make_numeric_identifier(created_stream.id),
                                          make_numeric_identifier(second_topic_id), "partition_id",
                                          partition_id_bytes(0), std::move(second_topic_messages)));

    const auto stream_before_purge = client->get_stream(make_string_identifier(stream_name));
    EXPECT_EQ(stream_before_purge.topics_count, 2u);
    EXPECT_EQ(stream_before_purge.messages_count, 5u);
    EXPECT_GT(stream_before_purge.size_bytes, 0u);

    std::unordered_map<std::string, std::uint64_t> messages_before_purge;
    for (const auto &topic : stream_before_purge.topics) {
        messages_before_purge[static_cast<std::string>(topic.name)] = topic.messages_count;
    }
    EXPECT_EQ(messages_before_purge[first_topic_name], 3u);
    EXPECT_EQ(messages_before_purge[second_topic_name], 2u);

    const auto streams_before_purge = client->get_streams();
    bool found_stream_before_purge  = false;
    for (const auto &stream : streams_before_purge) {
        if (static_cast<std::string>(stream.name) == stream_name) {
            found_stream_before_purge = true;
            EXPECT_EQ(stream.topics_count, 2u);
            EXPECT_EQ(stream.messages_count, 5u);
            EXPECT_GT(stream.size_bytes, 0u);
            break;
        }
    }
    ASSERT_TRUE(found_stream_before_purge);

    ASSERT_NO_THROW(client->purge_stream(make_string_identifier(stream_name)));

    const auto stream_after_purge = client->get_stream(make_string_identifier(stream_name));
    EXPECT_EQ(stream_after_purge.topics_count, 2u);
    EXPECT_EQ(stream_after_purge.messages_count, 0u);
    EXPECT_EQ(stream_after_purge.size_bytes, 0u);
    ASSERT_EQ(stream_after_purge.topics.size(), 2u);
    for (const auto &topic : stream_after_purge.topics) {
        EXPECT_EQ(topic.messages_count, 0u);
        EXPECT_EQ(topic.size_bytes, 0u);
    }

    const auto streams_after_purge = client->get_streams();
    bool found_stream_after_purge  = false;
    for (const auto &stream : streams_after_purge) {
        if (static_cast<std::string>(stream.name) == stream_name) {
            found_stream_after_purge = true;
            EXPECT_EQ(stream.topics_count, 2u);
            EXPECT_EQ(stream.messages_count, 0u);
            EXPECT_EQ(stream.size_bytes, 0u);
            break;
        }
    }
    ASSERT_TRUE(found_stream_after_purge);
}

TEST_F(LowLevelE2E_Stream, PurgeStreamAcrossMultipleTopicsAndPartitionsClearsEverything) {
    RecordProperty("description", "Purges all messages across multiple topics and partitions in the stream.");
    const std::string stream_name       = GetRandomName();
    const std::string first_topic_name  = GetRandomName();
    const std::string second_topic_name = GetRandomName();
    iggy::ffi::Client *client           = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), first_topic_name, 2, "none", 0,
                                         "server_default", 0, "server_default"));
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), second_topic_name, 3, "none", 0,
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

    for (std::uint32_t partition_id = 0; partition_id < 2; ++partition_id) {
        rust::Vec<iggy::ffi::IggyMessageToSend> messages;
        for (std::uint32_t i = 0; i < 2; ++i) {
            messages.push_back(iggy::ffi::make_message(
                to_payload("purge-stream-topic-a-" + std::to_string(partition_id) + "-" + std::to_string(i))));
        }
        ASSERT_NO_THROW(client->send_messages(make_numeric_identifier(created_stream.id),
                                              make_numeric_identifier(first_topic_id), "partition_id",
                                              partition_id_bytes(partition_id), std::move(messages)));
    }
    for (std::uint32_t partition_id = 0; partition_id < 3; ++partition_id) {
        rust::Vec<iggy::ffi::IggyMessageToSend> messages;
        for (std::uint32_t i = 0; i < 2; ++i) {
            messages.push_back(iggy::ffi::make_message(
                to_payload("purge-stream-topic-b-" + std::to_string(partition_id) + "-" + std::to_string(i))));
        }
        ASSERT_NO_THROW(client->send_messages(make_numeric_identifier(created_stream.id),
                                              make_numeric_identifier(second_topic_id), "partition_id",
                                              partition_id_bytes(partition_id), std::move(messages)));
    }

    const auto stream_before_purge = client->get_stream(make_string_identifier(stream_name));
    EXPECT_EQ(stream_before_purge.messages_count, 10u);

    ASSERT_NO_THROW(client->purge_stream(make_string_identifier(stream_name)));

    const auto stream_after_purge = client->get_stream(make_string_identifier(stream_name));
    EXPECT_EQ(stream_after_purge.topics_count, 2u);
    EXPECT_EQ(stream_after_purge.messages_count, 0u);
    EXPECT_EQ(stream_after_purge.size_bytes, 0u);
    for (const auto &topic : stream_after_purge.topics) {
        EXPECT_EQ(topic.messages_count, 0u);
        EXPECT_EQ(topic.size_bytes, 0u);
    }
}

TEST_F(LowLevelE2E_Stream, PurgeStreamThenSendMessagesAgainSucceeds) {
    RecordProperty("description", "Allows sending fresh messages to a topic after purging its parent stream.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));

    const auto created_stream = client->get_stream(make_string_identifier(stream_name));
    ASSERT_EQ(created_stream.topics.size(), 1u);
    const std::uint32_t topic_id = created_stream.topics.front().id;

    rust::Vec<iggy::ffi::IggyMessageToSend> first_batch;
    first_batch.push_back(iggy::ffi::make_message(to_payload("before-purge")));
    ASSERT_NO_THROW(client->send_messages(make_numeric_identifier(created_stream.id), make_numeric_identifier(topic_id),
                                          "partition_id", partition_id_bytes(0), std::move(first_batch)));

    ASSERT_NO_THROW(client->purge_stream(make_string_identifier(stream_name)));

    rust::Vec<iggy::ffi::IggyMessageToSend> second_batch;
    second_batch.push_back(iggy::ffi::make_message(to_payload("after-purge-0")));
    second_batch.push_back(iggy::ffi::make_message(to_payload("after-purge-1")));
    ASSERT_NO_THROW(client->send_messages(make_numeric_identifier(created_stream.id), make_numeric_identifier(topic_id),
                                          "partition_id", partition_id_bytes(0), std::move(second_batch)));

    const auto stream_after_resend = client->get_stream(make_string_identifier(stream_name));
    EXPECT_EQ(stream_after_resend.topics_count, 1u);
    EXPECT_EQ(stream_after_resend.messages_count, 2u);
    EXPECT_GT(stream_after_resend.size_bytes, 0u);
    ASSERT_EQ(stream_after_resend.topics.size(), 1u);
    EXPECT_EQ(stream_after_resend.topics.front().messages_count, 2u);
}

TEST_F(LowLevelE2E_Stream, PurgeStreamTwiceKeepsStreamEmptyAndTopicsIntact) {
    RecordProperty("description", "Allows purging the same stream twice and keeps the stream empty after both calls.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client->create_topic(make_string_identifier(stream_name), topic_name, 1, "none", 0,
                                         "server_default", 0, "server_default"));

    const auto created_stream = client->get_stream(make_string_identifier(stream_name));
    ASSERT_EQ(created_stream.topics.size(), 1u);
    const std::uint32_t topic_id = created_stream.topics.front().id;

    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    for (std::uint32_t i = 0; i < 3; ++i) {
        messages.push_back(iggy::ffi::make_message(to_payload("purge-stream-twice-" + std::to_string(i))));
    }
    ASSERT_NO_THROW(client->send_messages(make_numeric_identifier(created_stream.id), make_numeric_identifier(topic_id),
                                          "partition_id", partition_id_bytes(0), std::move(messages)));

    ASSERT_NO_THROW(client->purge_stream(make_string_identifier(stream_name)));
    const auto stream_after_first_purge = client->get_stream(make_string_identifier(stream_name));
    EXPECT_EQ(stream_after_first_purge.topics_count, 1u);
    EXPECT_EQ(stream_after_first_purge.messages_count, 0u);
    EXPECT_EQ(stream_after_first_purge.size_bytes, 0u);
    ASSERT_EQ(stream_after_first_purge.topics.size(), 1u);
    EXPECT_EQ(stream_after_first_purge.topics.front().messages_count, 0u);
    EXPECT_EQ(stream_after_first_purge.topics.front().size_bytes, 0u);

    ASSERT_NO_THROW(client->purge_stream(make_string_identifier(stream_name)));
    const auto stream_after_second_purge = client->get_stream(make_string_identifier(stream_name));
    EXPECT_EQ(stream_after_second_purge.topics_count, 1u);
    EXPECT_EQ(stream_after_second_purge.messages_count, 0u);
    EXPECT_EQ(stream_after_second_purge.size_bytes, 0u);
    ASSERT_EQ(stream_after_second_purge.topics.size(), 1u);
    EXPECT_EQ(stream_after_second_purge.topics.front().messages_count, 0u);
    EXPECT_EQ(stream_after_second_purge.topics.front().size_bytes, 0u);
}

TEST_F(LowLevelE2E_Stream, PurgeStreamBeforeLoginThrows) {
    RecordProperty("description", "Throws when stream purge is attempted before authentication.");
    const std::string stream_name = GetRandomName();

    iggy::ffi::Client *client = GetLoggedInClient();
    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);

    iggy::ffi::Client *unauthenticated_client = GetLoggedOutClient();

    ASSERT_THROW(unauthenticated_client->purge_stream(make_string_identifier(stream_name)), std::exception);
    ASSERT_NO_THROW(unauthenticated_client->connect());
    ASSERT_THROW(unauthenticated_client->purge_stream(make_string_identifier(stream_name)), std::exception);
}
