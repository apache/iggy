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

TEST_F(LowLevelE2E_Stream, UpdateStreamWorksCorrectly) {
    RecordProperty("description", "Updates an existing stream name while preserving the stream identity.");
    const std::string stream_name         = GetRandomName();
    const std::string updated_stream_name = GetRandomName();
    iggy::ffi::Client *client             = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);

    iggy::ffi::StreamDetails original_stream_details{};
    ASSERT_NO_THROW({ original_stream_details = client->get_stream(make_string_identifier(stream_name)); });
    const std::uint32_t stream_id = original_stream_details.id;
    ForgetTrackedStream(stream_name);
    TrackStream(stream_id);

    ASSERT_NO_THROW(client->update_stream(make_string_identifier(stream_name), updated_stream_name));

    ASSERT_THROW(client->get_stream(make_string_identifier(stream_name)), std::exception);

    iggy::ffi::StreamDetails updated_stream_details{};
    ASSERT_NO_THROW({ updated_stream_details = client->get_stream(make_numeric_identifier(stream_id)); });

    EXPECT_EQ(updated_stream_details.id, original_stream_details.id);
    EXPECT_EQ(updated_stream_details.created_at, original_stream_details.created_at);
    EXPECT_EQ(updated_stream_details.name, updated_stream_name);
    EXPECT_EQ(updated_stream_details.size_bytes, original_stream_details.size_bytes);
    EXPECT_EQ(updated_stream_details.messages_count, original_stream_details.messages_count);
    EXPECT_EQ(updated_stream_details.topics_count, original_stream_details.topics_count);
    EXPECT_EQ(updated_stream_details.topics.size(), original_stream_details.topics.size());
}

TEST_F(LowLevelE2E_Stream, UpdateStreamWithSameNameIsIdempotent) {
    RecordProperty("description",
                   "Calling update_stream with the current name succeeds without changing stream details.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);

    iggy::ffi::StreamDetails first_read{};
    iggy::ffi::StreamDetails second_read{};
    ASSERT_NO_THROW({ first_read = client->get_stream(make_string_identifier(stream_name)); });
    ASSERT_NO_THROW(client->update_stream(make_numeric_identifier(first_read.id), stream_name));
    ASSERT_NO_THROW({ second_read = client->get_stream(make_numeric_identifier(first_read.id)); });

    EXPECT_EQ(second_read.id, first_read.id);
    EXPECT_EQ(second_read.created_at, first_read.created_at);
    EXPECT_EQ(second_read.name, first_read.name);
    EXPECT_EQ(second_read.size_bytes, first_read.size_bytes);
    EXPECT_EQ(second_read.messages_count, first_read.messages_count);
    EXPECT_EQ(second_read.topics_count, first_read.topics_count);
    EXPECT_EQ(second_read.topics.size(), first_read.topics.size());
}

TEST_F(LowLevelE2E_Stream, UpdateStreamBeforeLoginThrows) {
    RecordProperty("description", "Rejects update_stream before connect, and after connect but before login.");
    const std::string stream_name         = GetRandomName();
    const std::string updated_stream_name = GetRandomName();
    iggy::ffi::Client *client             = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);

    iggy::ffi::Client *unauthenticated_client = GetLoggedOutClient();

    ASSERT_THROW(unauthenticated_client->update_stream(make_string_identifier(stream_name), updated_stream_name),
                 std::exception);
    ASSERT_NO_THROW(unauthenticated_client->connect());
    ASSERT_THROW(unauthenticated_client->update_stream(make_string_identifier(stream_name), updated_stream_name),
                 std::exception);
}

TEST_F(LowLevelE2E_Stream, UpdateStreamWithVariousUtf8Characters) {
    RecordProperty("description", "Updates a stream name with various UTF-8 values.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);

    std::uint32_t stream_id = 0;
    ASSERT_NO_THROW({
        const auto stream_details = client->get_stream(make_string_identifier(stream_name));
        stream_id                 = stream_details.id;
    });
    ForgetTrackedStream(stream_name);
    TrackStream(stream_id);

    const std::vector<std::string> updated_stream_names = {
        "こんにちは世界", "안녕하세요세계", "你好世界", "مرحبا بالعالم", "नमस्ते दुनिया", "🚀🍕✨🎯🔥",
    };

    for (const auto &updated_stream_name : updated_stream_names) {
        SCOPED_TRACE(updated_stream_name);
        ASSERT_NO_THROW(client->update_stream(make_numeric_identifier(stream_id), updated_stream_name));
        ASSERT_NO_THROW({
            const auto stream_details = client->get_stream(make_numeric_identifier(stream_id));
            EXPECT_EQ(stream_details.name, updated_stream_name);
        });
    }
}

TEST_F(LowLevelE2E_Stream, UpdateNonExistentStreamThrows) {
    RecordProperty("description", "Throws when updating a stream that does not exist.");
    const std::string stream_name         = GetRandomName();
    const std::string updated_stream_name = GetRandomName();
    iggy::ffi::Client *client             = GetLoggedInClient();

    ASSERT_THROW(client->update_stream(make_string_identifier(stream_name), updated_stream_name), std::exception);
}

TEST_F(LowLevelE2E_Stream, UpdateStreamWithDuplicateNameThrows) {
    RecordProperty("description", "Rejects renaming a stream to another stream's existing name.");
    const std::string first_stream_name  = GetRandomName();
    const std::string second_stream_name = GetRandomName();
    iggy::ffi::Client *client            = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(first_stream_name));
    TrackStream(first_stream_name);
    ASSERT_NO_THROW(client->create_stream(second_stream_name));
    TrackStream(second_stream_name);

    ASSERT_THROW(client->update_stream(make_string_identifier(first_stream_name), second_stream_name), std::exception);
}

TEST_F(LowLevelE2E_Stream, UpdateDeletedStreamThrows) {
    RecordProperty("description", "Throws when updating a stream after it has been deleted.");
    const std::string stream_name         = GetRandomName();
    const std::string updated_stream_name = GetRandomName();
    iggy::ffi::Client *client             = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);

    ASSERT_NO_THROW(client->delete_stream(make_string_identifier(stream_name)));
    ForgetTrackedStream(stream_name);

    ASSERT_THROW(client->update_stream(make_string_identifier(stream_name), updated_stream_name), std::exception);
}

TEST_F(LowLevelE2E_Stream, UpdateStreamFailedValidationDoesNotMutateStream) {
    RecordProperty("description", "Keeps the stream unchanged when update_stream fails wrapper validation.");
    const std::string stream_name         = GetRandomName();
    const std::string updated_stream_name = GetRandomName();
    iggy::ffi::Client *client             = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);

    iggy::ffi::StreamDetails stream_before_failed_update{};
    ASSERT_NO_THROW({ stream_before_failed_update = client->get_stream(make_string_identifier(stream_name)); });

    iggy::ffi::Identifier invalid_numeric_id;
    invalid_numeric_id.kind   = "numeric";
    invalid_numeric_id.length = 1;
    invalid_numeric_id.value.push_back(1);
    ASSERT_THROW(client->update_stream(std::move(invalid_numeric_id), updated_stream_name), std::exception);

    iggy::ffi::StreamDetails stream_after_failed_update{};
    ASSERT_NO_THROW({ stream_after_failed_update = client->get_stream(make_string_identifier(stream_name)); });

    EXPECT_EQ(stream_after_failed_update.id, stream_before_failed_update.id);
    EXPECT_EQ(stream_after_failed_update.created_at, stream_before_failed_update.created_at);
    EXPECT_EQ(stream_after_failed_update.name, stream_before_failed_update.name);
    EXPECT_EQ(stream_after_failed_update.size_bytes, stream_before_failed_update.size_bytes);
    EXPECT_EQ(stream_after_failed_update.messages_count, stream_before_failed_update.messages_count);
    EXPECT_EQ(stream_after_failed_update.topics_count, stream_before_failed_update.topics_count);
    EXPECT_EQ(stream_after_failed_update.topics.size(), stream_before_failed_update.topics.size());

    ASSERT_THROW(client->get_stream(make_string_identifier(updated_stream_name)), std::exception);
}

TEST_F(LowLevelE2E_Stream, UpdateStreamOnlyChangesName) {
    RecordProperty(
        "description",
        "Changes only the stream name and leaves stream, topic, message, partition, and segment data intact.");
    const std::string stream_name         = GetRandomName();
    const std::string updated_stream_name = GetRandomName();
    const std::string topic_name          = GetRandomName();
    iggy::ffi::Client *client             = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);

    std::uint32_t stream_id = 0;
    ASSERT_NO_THROW({
        const auto stream_details = client->get_stream(make_string_identifier(stream_name));
        stream_id                 = stream_details.id;
    });
    ForgetTrackedStream(stream_name);
    TrackStream(stream_id);

    ASSERT_NO_THROW(client->create_topic(make_numeric_identifier(stream_id), topic_name, 2, "none", 0, "never_expire",
                                         0, "server_default"));

    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    for (std::uint32_t i = 0; i < 3; ++i) {
        auto message = iggy::ffi::make_message(to_payload("stream-update-preserve-" + std::to_string(i)));
        messages.push_back(std::move(message));
    }
    ASSERT_NO_THROW(client->send_messages(make_numeric_identifier(stream_id), make_numeric_identifier(0),
                                          "partition_id", partition_id_bytes(0), std::move(messages)));

    iggy::ffi::StreamDetails stream_before_update{};
    iggy::ffi::StreamDetails stream_after_update{};
    iggy::ffi::Stats stats_before_update{};
    iggy::ffi::Stats stats_after_update{};
    ASSERT_NO_THROW({
        stream_before_update = client->get_stream(make_numeric_identifier(stream_id));
        stats_before_update  = client->get_stats();
    });

    ASSERT_NO_THROW(client->update_stream(make_numeric_identifier(stream_id), updated_stream_name));

    ASSERT_THROW(client->get_stream(make_string_identifier(stream_name)), std::exception);
    ASSERT_NO_THROW({
        stream_after_update = client->get_stream(make_numeric_identifier(stream_id));
        stats_after_update  = client->get_stats();
    });

    EXPECT_EQ(stream_after_update.id, stream_before_update.id);
    EXPECT_EQ(stream_after_update.created_at, stream_before_update.created_at);
    EXPECT_EQ(stream_after_update.name, updated_stream_name);
    EXPECT_EQ(stream_after_update.size_bytes, stream_before_update.size_bytes);
    EXPECT_EQ(stream_after_update.messages_count, stream_before_update.messages_count);
    EXPECT_EQ(stream_after_update.topics_count, stream_before_update.topics_count);
    ASSERT_EQ(stream_before_update.topics.size(), 1u);
    ASSERT_EQ(stream_after_update.topics.size(), 1u);

    const auto &before_topic = stream_before_update.topics[0];
    const auto &after_topic  = stream_after_update.topics[0];
    EXPECT_EQ(after_topic.id, before_topic.id);
    EXPECT_EQ(after_topic.created_at, before_topic.created_at);
    EXPECT_EQ(after_topic.name, before_topic.name);
    EXPECT_EQ(after_topic.size_bytes, before_topic.size_bytes);
    EXPECT_EQ(after_topic.message_expiry, before_topic.message_expiry);
    EXPECT_EQ(after_topic.compression_algorithm, before_topic.compression_algorithm);
    EXPECT_EQ(after_topic.max_topic_size, before_topic.max_topic_size);
    EXPECT_EQ(after_topic.replication_factor, before_topic.replication_factor);
    EXPECT_EQ(after_topic.messages_count, before_topic.messages_count);
    EXPECT_EQ(after_topic.partitions_count, before_topic.partitions_count);

    EXPECT_EQ(stats_after_update.streams_count, stats_before_update.streams_count);
    EXPECT_EQ(stats_after_update.topics_count, stats_before_update.topics_count);
    EXPECT_EQ(stats_after_update.partitions_count, stats_before_update.partitions_count);
    EXPECT_EQ(stats_after_update.segments_count, stats_before_update.segments_count);
    EXPECT_EQ(stats_after_update.messages_count, stats_before_update.messages_count);
}

TEST_F(LowLevelE2E_Stream, UpdateStreamValidatesNameBounds) {
    RecordProperty("description",
                   "Rejects invalid stream name lengths during update and accepts the maximum allowed name length.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    ASSERT_NO_THROW(client->create_stream(stream_name));
    TrackStream(stream_name);

    std::uint32_t stream_id = 0;
    ASSERT_NO_THROW({
        const auto stream_details = client->get_stream(make_string_identifier(stream_name));
        stream_id                 = stream_details.id;
    });
    ForgetTrackedStream(stream_name);
    TrackStream(stream_id);

    const std::vector<std::string> invalid_stream_names = {
        "",
        std::string(256, 'b'),
    };
    for (const auto &invalid_stream_name : invalid_stream_names) {
        SCOPED_TRACE("invalid_stream_name_length=" + std::to_string(invalid_stream_name.size()));
        ASSERT_THROW(client->update_stream(make_numeric_identifier(stream_id), invalid_stream_name), std::exception);
    }
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
