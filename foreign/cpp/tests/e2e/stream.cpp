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
#include "tests/e2e/test_helpers.hpp"

class E2E_Stream : public E2ETestFixture {};

TEST_F(E2E_Stream, CreateStreamAfterLogin) {
    RecordProperty("description", "Creates a stream successfully after authenticating.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
}

TEST_F(E2E_Stream, CreateDuplicateStreamThrows) {
    RecordProperty("description", "Rejects creating the same stream twice.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_THROW(client.CreateStream(stream_name), std::exception);
}

TEST_F(E2E_Stream, CreateStreamBeforeLoginThrows) {
    RecordProperty("description", "Throws when stream creation is attempted before authentication.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedOutHighLevelClient();

    ASSERT_THROW(client.CreateStream(stream_name), std::exception);
    ASSERT_NO_THROW(client.Connect());
    ASSERT_THROW(client.CreateStream(stream_name), std::exception);
    ASSERT_NO_THROW(client.Login("iggy", "iggy"));
    ASSERT_NO_THROW(client.Disconnect());
    ASSERT_THROW(client.CreateStream(stream_name), std::exception);
}

TEST_F(E2E_Stream, CreateStreamValidatesNameConstraintsAndUniqueness) {
    RecordProperty("description",
                   "Validates stream name length constraints and accepts the maximum allowed name length.");
    const std::string illegal_stream_names[] = {
        "",
        std::string(256, 'b'),
    };
    auto client = GetLoggedInHighLevelClient();
    for (const auto &stream_name : illegal_stream_names) {
        SCOPED_TRACE(stream_name);
        ASSERT_THROW(client.CreateStream(stream_name), std::exception);
    }

    const std::string max_length_name(255, 'a');
    ASSERT_NO_THROW(client.CreateStream(max_length_name));
    TrackStream(max_length_name);
}

TEST_F(E2E_Stream, CreateStreamWithEmojiName) {
    RecordProperty("description", "Creates a stream with a UTF-8 emoji name.");
    const std::string stream_name = "🚀🚀🚀🚀Apache Iggy🚀🚀🚀🚀";
    auto client                   = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW({
        const auto stream_details = client.GetStream(iggy::Identifier::String(stream_name));
        EXPECT_EQ(stream_details.Name(), stream_name);
        EXPECT_EQ(stream_details.TopicsCount(), 0u);
        EXPECT_EQ(stream_details.Topics().size(), 0u);
    });
}

TEST_F(E2E_Stream, UpdateStreamWorksCorrectly) {
    RecordProperty("description", "Updates an existing stream name while preserving the stream identity.");
    const std::string stream_name         = GetRandomName();
    const std::string updated_stream_name = GetRandomName();
    auto client                           = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);

    iggy::StreamDetails original_stream_details = client.GetStream(iggy::Identifier::String(stream_name));
    const std::uint32_t stream_id               = original_stream_details.Id();
    ForgetTrackedStream(stream_name);
    TrackStream(stream_id);

    ASSERT_NO_THROW(client.UpdateStream(iggy::Identifier::String(stream_name), updated_stream_name));

    ASSERT_THROW(client.GetStream(iggy::Identifier::String(stream_name)), std::exception);

    iggy::StreamDetails updated_stream_details = client.GetStream(iggy::Identifier::Numeric(stream_id));

    EXPECT_EQ(updated_stream_details.Id(), original_stream_details.Id());
    EXPECT_EQ(updated_stream_details.CreatedAt(), original_stream_details.CreatedAt());
    EXPECT_EQ(updated_stream_details.Name(), updated_stream_name);
    EXPECT_EQ(updated_stream_details.SizeBytes(), original_stream_details.SizeBytes());
    EXPECT_EQ(updated_stream_details.MessagesCount(), original_stream_details.MessagesCount());
    EXPECT_EQ(updated_stream_details.TopicsCount(), original_stream_details.TopicsCount());
    EXPECT_EQ(updated_stream_details.Topics().size(), original_stream_details.Topics().size());
}

TEST_F(E2E_Stream, UpdateStreamWithSameNameIsIdempotent) {
    RecordProperty("description",
                   "Calling update_stream with the current name succeeds without changing stream details.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);

    auto first_read = client.GetStream(iggy::Identifier::String(stream_name));
    ASSERT_NO_THROW(client.UpdateStream(iggy::Identifier::Numeric(first_read.Id()), stream_name));
    auto second_read = client.GetStream(iggy::Identifier::Numeric(first_read.Id()));

    EXPECT_EQ(second_read.Id(), first_read.Id());
    EXPECT_EQ(second_read.CreatedAt(), first_read.CreatedAt());
    EXPECT_EQ(second_read.Name(), first_read.Name());
    EXPECT_EQ(second_read.SizeBytes(), first_read.SizeBytes());
    EXPECT_EQ(second_read.MessagesCount(), first_read.MessagesCount());
    EXPECT_EQ(second_read.TopicsCount(), first_read.TopicsCount());
    EXPECT_EQ(second_read.Topics().size(), first_read.Topics().size());
}

TEST_F(E2E_Stream, UpdateStreamBeforeLoginThrows) {
    RecordProperty("description", "Rejects update_stream before connect, and after connect but before login.");
    const std::string stream_name         = GetRandomName();
    const std::string updated_stream_name = GetRandomName();
    auto client                           = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);

    auto unauthenticated_client = GetLoggedOutHighLevelClient();

    ASSERT_THROW(unauthenticated_client.UpdateStream(iggy::Identifier::String(stream_name), updated_stream_name),
                 std::exception);
    ASSERT_NO_THROW(unauthenticated_client.Connect());
    ASSERT_THROW(unauthenticated_client.UpdateStream(iggy::Identifier::String(stream_name), updated_stream_name),
                 std::exception);
    ASSERT_NO_THROW(unauthenticated_client.Login("iggy", "iggy"));
    ASSERT_NO_THROW(unauthenticated_client.Disconnect());
    ASSERT_THROW(unauthenticated_client.UpdateStream(iggy::Identifier::String(stream_name), updated_stream_name),
                 std::exception);
}

TEST_F(E2E_Stream, UpdateStreamWithVariousUtf8Characters) {
    RecordProperty("description", "Updates a stream name with various UTF-8 values.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);

    std::uint32_t stream_id = 0;
    ASSERT_NO_THROW({
        const auto stream_details = client.GetStream(iggy::Identifier::String(stream_name));
        stream_id                 = stream_details.Id();
    });
    ForgetTrackedStream(stream_name);
    TrackStream(stream_id);

    const std::vector<std::string> updated_stream_names = {
        "こんにちは世界", "안녕하세요세계", "你好世界", "مرحبا بالعالم", "नमस्ते दुनिया", "🚀🍕✨🎯🔥",
    };

    for (const auto &updated_stream_name : updated_stream_names) {
        SCOPED_TRACE(updated_stream_name);
        ASSERT_NO_THROW(client.UpdateStream(iggy::Identifier::Numeric(stream_id), updated_stream_name));
        ASSERT_NO_THROW({
            const auto stream_details = client.GetStream(iggy::Identifier::Numeric(stream_id));
            EXPECT_EQ(stream_details.Name(), updated_stream_name);
        });
    }
}

TEST_F(E2E_Stream, UpdateNonExistentStreamThrows) {
    RecordProperty("description", "Throws when updating a stream that does not exist.");
    const std::string stream_name         = GetRandomName();
    const std::string updated_stream_name = GetRandomName();
    auto client                           = GetLoggedInHighLevelClient();
    ASSERT_THROW(client.UpdateStream(iggy::Identifier::String(stream_name), updated_stream_name), std::exception);
}

TEST_F(E2E_Stream, UpdateStreamWithDuplicateNameThrows) {
    RecordProperty("description", "Rejects renaming a stream to another stream's existing name.");
    const std::string first_stream_name  = GetRandomName();
    const std::string second_stream_name = GetRandomName();
    auto client                          = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(first_stream_name));
    TrackStream(first_stream_name);
    ASSERT_NO_THROW(client.CreateStream(second_stream_name));
    TrackStream(second_stream_name);

    ASSERT_THROW(client.UpdateStream(iggy::Identifier::String(first_stream_name), second_stream_name), std::exception);
}

TEST_F(E2E_Stream, UpdateDeletedStreamThrows) {
    RecordProperty("description", "Throws when updating a stream after it has been deleted.");
    const std::string stream_name         = GetRandomName();
    const std::string updated_stream_name = GetRandomName();
    auto client                           = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);

    ASSERT_NO_THROW(client.DeleteStream(iggy::Identifier::String(stream_name)));
    ForgetTrackedStream(stream_name);

    ASSERT_THROW(client.UpdateStream(iggy::Identifier::String(stream_name), updated_stream_name), std::exception);
}

TEST_F(E2E_Stream, UpdateStreamFailedValidationDoesNotMutateStream) {
    RecordProperty("description", "Keeps the stream unchanged when update_stream fails wrapper validation.");
    const std::string stream_name         = GetRandomName();
    const std::string updated_stream_name = GetRandomName();
    auto client                           = GetLoggedInHighLevelClient();
    iggy::ffi::Client *ffi_client         = GetLoggedInClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);

    auto stream_before_failed_update = client.GetStream(iggy::Identifier::String(stream_name));

    iggy::ffi::Identifier invalid_numeric_id;
    invalid_numeric_id.kind   = "numeric";
    invalid_numeric_id.length = 1;
    invalid_numeric_id.value.push_back(1);
    ASSERT_THROW(ffi_client->update_stream(std::move(invalid_numeric_id), updated_stream_name), std::exception);

    auto stream_after_failed_update = client.GetStream(iggy::Identifier::String(stream_name));

    EXPECT_EQ(stream_after_failed_update.Id(), stream_before_failed_update.Id());
    EXPECT_EQ(stream_after_failed_update.CreatedAt(), stream_before_failed_update.CreatedAt());
    EXPECT_EQ(stream_after_failed_update.Name(), stream_before_failed_update.Name());
    EXPECT_EQ(stream_after_failed_update.SizeBytes(), stream_before_failed_update.SizeBytes());
    EXPECT_EQ(stream_after_failed_update.MessagesCount(), stream_before_failed_update.MessagesCount());
    EXPECT_EQ(stream_after_failed_update.TopicsCount(), stream_before_failed_update.TopicsCount());
    EXPECT_EQ(stream_after_failed_update.Topics().size(), stream_before_failed_update.Topics().size());

    ASSERT_THROW(client.GetStream(iggy::Identifier::String(updated_stream_name)), std::exception);
}

TEST_F(E2E_Stream, UpdateStreamOnlyChangesName) {
    RecordProperty(
        "description",
        "Changes only the stream name and leaves stream, topic, message, partition, and segment data intact.");
    const std::string stream_name         = GetRandomName();
    const std::string updated_stream_name = GetRandomName();
    const std::string topic_name          = GetRandomName();
    auto client                           = GetLoggedInHighLevelClient();
    iggy::ffi::Client *ffi_client         = GetLoggedInClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);

    std::uint32_t stream_id = 0;
    ASSERT_NO_THROW({
        const auto stream_details = client.GetStream(iggy::Identifier::String(stream_name));
        stream_id                 = stream_details.Id();
    });
    ForgetTrackedStream(stream_name);
    TrackStream(stream_id);

    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::Numeric(stream_id), topic_name, 2,
                                       iggy::CompressionAlgorithm::None(), iggy::Expiry::NeverExpire()));

    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    for (std::uint32_t i = 0; i < 3; ++i) {
        auto message = iggy::ffi::make_message(to_payload("stream-update-preserve-" + std::to_string(i)),
                                               rust::Vec<iggy::ffi::HeaderEntry>());
        messages.push_back(std::move(message));
    }
    ASSERT_NO_THROW(ffi_client->send_messages(make_numeric_identifier(stream_id), make_numeric_identifier(0),
                                              "partition_id", partition_id_bytes(0), std::move(messages)));

    auto stream_before_update = client.GetStream(iggy::Identifier::Numeric(stream_id));
    iggy::ffi::Stats stats_before_update{};
    ASSERT_NO_THROW({ stats_before_update = ffi_client->get_stats(); });

    ASSERT_NO_THROW(client.UpdateStream(iggy::Identifier::Numeric(stream_id), updated_stream_name));

    ASSERT_THROW(client.GetStream(iggy::Identifier::String(stream_name)), std::exception);
    auto stream_after_update = client.GetStream(iggy::Identifier::Numeric(stream_id));
    iggy::ffi::Stats stats_after_update{};
    ASSERT_NO_THROW({ stats_after_update = ffi_client->get_stats(); });

    EXPECT_EQ(stream_after_update.Id(), stream_before_update.Id());
    EXPECT_EQ(stream_after_update.CreatedAt(), stream_before_update.CreatedAt());
    EXPECT_EQ(stream_after_update.Name(), updated_stream_name);
    EXPECT_EQ(stream_after_update.SizeBytes(), stream_before_update.SizeBytes());
    EXPECT_EQ(stream_after_update.MessagesCount(), stream_before_update.MessagesCount());
    EXPECT_EQ(stream_after_update.TopicsCount(), stream_before_update.TopicsCount());
    ASSERT_EQ(stream_before_update.Topics().size(), 1u);
    ASSERT_EQ(stream_after_update.Topics().size(), 1u);

    const auto &before_topic = stream_before_update.Topics()[0];
    const auto &after_topic  = stream_after_update.Topics()[0];
    EXPECT_EQ(after_topic.Id(), before_topic.Id());
    EXPECT_EQ(after_topic.CreatedAt(), before_topic.CreatedAt());
    EXPECT_EQ(after_topic.Name(), before_topic.Name());
    EXPECT_EQ(after_topic.SizeBytes(), before_topic.SizeBytes());
    EXPECT_EQ(after_topic.MessageExpiry(), before_topic.MessageExpiry());
    EXPECT_EQ(after_topic.CompressionAlgorithm(), before_topic.CompressionAlgorithm());
    EXPECT_EQ(after_topic.MaxTopicSize(), before_topic.MaxTopicSize());
    EXPECT_EQ(after_topic.MessagesCount(), before_topic.MessagesCount());
    EXPECT_EQ(after_topic.PartitionsCount(), before_topic.PartitionsCount());

    EXPECT_EQ(stats_after_update.streams_count, stats_before_update.streams_count);
    EXPECT_EQ(stats_after_update.topics_count, stats_before_update.topics_count);
    EXPECT_EQ(stats_after_update.partitions_count, stats_before_update.partitions_count);
    EXPECT_EQ(stats_after_update.segments_count, stats_before_update.segments_count);
    EXPECT_EQ(stats_after_update.messages_count, stats_before_update.messages_count);
}

TEST_F(E2E_Stream, UpdateStreamValidatesNameBounds) {
    RecordProperty("description",
                   "Rejects invalid stream name lengths during update and accepts the maximum allowed name length.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);

    std::uint32_t stream_id = 0;
    ASSERT_NO_THROW({
        const auto stream_details = client.GetStream(iggy::Identifier::String(stream_name));
        stream_id                 = stream_details.Id();
    });
    ForgetTrackedStream(stream_name);
    TrackStream(stream_id);

    const std::vector<std::string> invalid_stream_names = {
        "",
        std::string(256, 'b'),
    };
    for (const auto &invalid_stream_name : invalid_stream_names) {
        SCOPED_TRACE("invalid_stream_name_length=" + std::to_string(invalid_stream_name.size()));
        ASSERT_THROW(client.UpdateStream(iggy::Identifier::Numeric(stream_id), invalid_stream_name), std::exception);
    }
}

TEST_F(E2E_Stream, StreamCreatedAndDeletedSuccessfully) {
    RecordProperty("description", "Creates a stream and deletes it successfully by string identifier.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);

    ASSERT_NO_THROW(client.DeleteStream(iggy::Identifier::String(stream_name)));
    ForgetTrackedStream(stream_name);
}

TEST_F(E2E_Stream, DeleteNotCreatedStreamThrows) {
    RecordProperty("description", "Throws when deleting a stream that does not exist.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();
    ASSERT_THROW(client.DeleteStream(iggy::Identifier::String(stream_name)), std::exception);
}

TEST_F(E2E_Stream, DeleteStreamBeforeLoginThrows) {
    RecordProperty("description", "Throws when stream deletion is attempted before authentication.");
    const std::string stream_name = GetRandomName();

    auto client = GetLoggedOutHighLevelClient();

    ASSERT_THROW(client.DeleteStream(iggy::Identifier::String(stream_name)), std::exception);

    ASSERT_NO_THROW(client.Connect());

    ASSERT_THROW(client.DeleteStream(iggy::Identifier::String(stream_name)), std::exception);
    ASSERT_NO_THROW(client.Login("iggy", "iggy"));
    ASSERT_NO_THROW(client.Disconnect());
    ASSERT_THROW(client.DeleteStream(iggy::Identifier::String(stream_name)), std::exception);
}

TEST_F(E2E_Stream, DeleteStreamTwiceThrows) {
    RecordProperty("description", "Throws when deleting the same stream a second time.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);

    ASSERT_NO_THROW(client.DeleteStream(iggy::Identifier::String(stream_name)));
    ForgetTrackedStream(stream_name);
    ASSERT_THROW(client.DeleteStream(iggy::Identifier::String(stream_name)), std::exception);
}

TEST_F(E2E_Stream, GetStreamByStringIdentifierReturnsStreamDetails) {
    RecordProperty("description", "Returns expected stream details when looked up by string identifier.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);

    ASSERT_NO_THROW({
        const auto stream_details = client.GetStream(iggy::Identifier::String(stream_name));
        EXPECT_EQ(stream_details.Name(), stream_name);
        EXPECT_EQ(stream_details.TopicsCount(), 0u);
        EXPECT_EQ(stream_details.Topics().size(), 0u);
        EXPECT_EQ(stream_details.MessagesCount(), 0u);
        EXPECT_EQ(stream_details.SizeBytes(), 0u);
    });
}

TEST_F(E2E_Stream, GetNonExistentStreamDetailsThrows) {
    RecordProperty("description", "Throws when requesting details for a stream that does not exist.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();
    ASSERT_THROW(client.GetStream(iggy::Identifier::String(stream_name)), std::exception);
}

TEST_F(E2E_Stream, GetStreamDetailsBeforeLoginThrows) {
    RecordProperty("description", "Throws when stream details are requested before authentication.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedOutHighLevelClient();

    ASSERT_THROW(client.GetStream(iggy::Identifier::String(stream_name)), std::exception);
    ASSERT_NO_THROW(client.Connect());
    ASSERT_THROW(client.GetStream(iggy::Identifier::String(stream_name)), std::exception);
    ASSERT_NO_THROW(client.Login("iggy", "iggy"));
    ASSERT_NO_THROW(client.Disconnect());
    ASSERT_THROW(client.GetStream(iggy::Identifier::String(stream_name)), std::exception);
}

TEST_F(E2E_Stream, GetDeletedStreamDetailsThrows) {
    RecordProperty("description", "Throws when requesting details for a stream after it has been deleted.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.GetStream(iggy::Identifier::String(stream_name)));
    ASSERT_NO_THROW(client.DeleteStream(iggy::Identifier::String(stream_name)));
    ForgetTrackedStream(stream_name);
    ASSERT_THROW(client.GetStream(iggy::Identifier::String(stream_name)), std::exception);
}

TEST_F(E2E_Stream, GetStreamsReturnsEmptyAfterCleanup) {
    RecordProperty("description", "Verifies get_streams returns empty vector after cleaning up all streams.");
    auto client  = GetLoggedInHighLevelClient();
    auto streams = client.GetStreams();
    for (const auto &s : streams) {
        client.DeleteStream(iggy::Identifier::Numeric(s.Id()));
    }

    streams = client.GetStreams();
    ASSERT_EQ(streams.size(), 0u);
}

TEST_F(E2E_Stream, GetStreamsReturnsStreamAfterCreation) {
    RecordProperty("description", "Verifies created stream appears in get_streams result.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();
    client.CreateStream(stream_name);
    TrackStream(stream_name);
    auto streams = client.GetStreams();
    ASSERT_GE(streams.size(), 1u);

    bool found = false;
    for (const auto &s : streams) {
        if (s.Name() == stream_name) {
            found = true;
            EXPECT_GT(s.CreatedAt(), 0u);
            EXPECT_EQ(s.SizeBytes(), 0u);
            EXPECT_EQ(s.MessagesCount(), 0u);
            EXPECT_EQ(s.TopicsCount(), 0u);
            break;
        }
    }
    ASSERT_TRUE(found) << "Stream '" << stream_name << "' not found in get_streams result";
}

TEST_F(E2E_Stream, GetStreamsFieldsVerification) {
    RecordProperty("description",
                   "Verifies get_streams returns correct field values after creating stream with topic and messages.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();
    iggy::ffi::Client *ffi_client = GetLoggedInClient();
    client.CreateStream(stream_name);
    TrackStream(stream_name);
    auto stream                  = client.GetStream(iggy::Identifier::String(stream_name));
    const std::string topic_name = GetRandomName();
    client.CreateTopic(iggy::Identifier::Numeric(stream.Id()), topic_name, 1, iggy::CompressionAlgorithm::None(),
                       iggy::Expiry::NeverExpire());

    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    for (std::uint32_t i = 0; i < 5; i++) {
        auto msg = iggy::ffi::make_message(to_payload("field-verify-message-" + std::to_string(i)),
                                           rust::Vec<iggy::ffi::HeaderEntry>());
        messages.push_back(std::move(msg));
    }
    ffi_client->send_messages(make_numeric_identifier(stream.Id()), make_numeric_identifier(0), "partition_id",
                              partition_id_bytes(0), std::move(messages));

    auto streams = client.GetStreams();
    ASSERT_GE(streams.size(), 1u);

    bool found = false;
    for (const auto &s : streams) {
        if (s.Name() == stream_name) {
            found = true;
            EXPECT_EQ(s.TopicsCount(), 1u);
            EXPECT_EQ(s.MessagesCount(), 5u);
            break;
        }
    }
    ASSERT_TRUE(found) << "Stream '" << stream_name << "' not found in get_streams result";
}

TEST_F(E2E_Stream, GetStreamsBeforeLoginThrows) {
    RecordProperty("description", "Throws when get_streams is called before authentication.");
    auto client = GetLoggedOutHighLevelClient();

    ASSERT_THROW(client.GetStreams(), std::exception);
    ASSERT_NO_THROW(client.Connect());
    ASSERT_THROW(client.GetStreams(), std::exception);
    ASSERT_NO_THROW(client.Login("iggy", "iggy"));
    ASSERT_NO_THROW(client.Disconnect());
    ASSERT_THROW(client.GetStreams(), std::exception);
}

TEST_F(E2E_Stream, GetStreamsConsistentWithGetStream) {
    RecordProperty("description", "Verifies get_streams result is consistent with get_stream for the same stream.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();
    client.CreateStream(stream_name);
    TrackStream(stream_name);

    std::string list_name;
    std::uint32_t list_id           = 0;
    std::uint32_t list_topics_count = 0;
    std::uint64_t list_created_at   = 0;
    std::uint64_t list_size_bytes   = 0;
    auto streams                    = client.GetStreams();
    for (const auto &s : streams) {
        if (s.Name() == stream_name) {
            list_name         = s.Name();
            list_id           = s.Id();
            list_topics_count = s.TopicsCount();
            list_created_at   = s.CreatedAt();
            list_size_bytes   = s.SizeBytes();
            break;
        }
    }
    ASSERT_FALSE(list_name.empty()) << "Stream '" << stream_name << "' not found in get_streams result";

    auto single        = client.GetStream(iggy::Identifier::String(stream_name));
    auto single_name   = single.Name();
    auto single_topics = single.TopicsCount();

    EXPECT_EQ(list_name, single_name);
    EXPECT_EQ(list_id, single.Id());
    EXPECT_EQ(list_topics_count, single_topics);
    EXPECT_EQ(list_created_at, single.CreatedAt());
    EXPECT_EQ(list_size_bytes, single.SizeBytes());
}

TEST_F(E2E_Stream, GetStreamsRepeatedCallsReturnSameResult) {
    RecordProperty("description", "Verifies repeated get_streams calls return consistent results.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();
    client.CreateStream(stream_name);
    TrackStream(stream_name);

    auto streams1 = client.GetStreams();
    auto streams2 = client.GetStreams();
    auto streams3 = client.GetStreams();

    ASSERT_EQ(streams1.size(), streams2.size());
    ASSERT_EQ(streams2.size(), streams3.size());

    auto contains_stream = [&](const std::vector<iggy::Stream> &vec) {
        for (const auto &s : vec) {
            if (s.Name() == stream_name) {
                return true;
            }
        }
        return false;
    };

    ASSERT_TRUE(contains_stream(streams1)) << "Stream not found in first call";
    ASSERT_TRUE(contains_stream(streams2)) << "Stream not found in second call";
    ASSERT_TRUE(contains_stream(streams3)) << "Stream not found in third call";
}

TEST_F(E2E_Stream, PurgeStreamOnNonExistentStreamThrows) {
    RecordProperty("description", "Throws when purging a stream that does not exist.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();
    ASSERT_THROW(client.PurgeStream(iggy::Identifier::String(stream_name)), std::exception);
}

TEST_F(E2E_Stream, PurgeStreamAfterStreamDeletionThrows) {
    RecordProperty("description", "Throws when purging a stream after it has been deleted.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.DeleteStream(iggy::Identifier::String(stream_name)));
    ForgetTrackedStream(stream_name);

    ASSERT_THROW(client.PurgeStream(iggy::Identifier::String(stream_name)), std::exception);
}

TEST_F(E2E_Stream, PurgeStreamPreservesStreamMetadata) {
    RecordProperty("description", "Preserves stream identity and topic metadata after purging stream messages.");
    const std::string stream_name       = GetRandomName();
    const std::string first_topic_name  = GetRandomName();
    const std::string second_topic_name = GetRandomName();
    auto client                         = GetLoggedInHighLevelClient();
    iggy::ffi::Client *ffi_client       = GetLoggedInClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), first_topic_name, 2,
                                       iggy::CompressionAlgorithm::Gzip(), iggy::Expiry::Duration(1000),
                                       iggy::MaxTopicSize::FromBytes(1024ULL * 1024ULL * 1024ULL)));
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), second_topic_name, 3,
                                       iggy::CompressionAlgorithm::None(), iggy::Expiry::NeverExpire()));

    const auto stream_before_purge = client.GetStream(iggy::Identifier::String(stream_name));
    ASSERT_EQ(stream_before_purge.Topics().size(), 2u);

    rust::Vec<iggy::ffi::IggyMessageToSend> first_topic_messages;
    first_topic_messages.push_back(
        iggy::ffi::make_message(to_payload("preserve-stream-metadata"), rust::Vec<iggy::ffi::HeaderEntry>()));
    ASSERT_NO_THROW(ffi_client->send_messages(make_numeric_identifier(stream_before_purge.Id()),
                                              make_string_identifier(first_topic_name), "partition_id",
                                              partition_id_bytes(0), std::move(first_topic_messages)));

    const auto stream_with_messages = client.GetStream(iggy::Identifier::String(stream_name));
    EXPECT_GT(stream_with_messages.MessagesCount(), 0u);
    EXPECT_GT(stream_with_messages.SizeBytes(), 0u);

    struct TopicMetadata {
        std::uint32_t id;
        std::uint64_t created_at;
        std::string name;
        std::uint64_t message_expiry;
        std::string compression_algorithm;
        std::uint64_t max_topic_size;
        std::uint32_t partitions_count;
    };
    std::unordered_map<std::string, TopicMetadata> topics_before_purge;
    for (const auto &topic : stream_with_messages.Topics()) {
        topics_before_purge[topic.Name()] = {topic.Id(),
                                             topic.CreatedAt(),
                                             topic.Name(),
                                             topic.MessageExpiry(),
                                             topic.CompressionAlgorithm(),
                                             topic.MaxTopicSize(),
                                             topic.PartitionsCount()};
    }

    ASSERT_NO_THROW(client.PurgeStream(iggy::Identifier::String(stream_name)));

    const auto stream_after_purge = client.GetStream(iggy::Identifier::String(stream_name));
    EXPECT_EQ(stream_after_purge.Id(), stream_with_messages.Id());
    EXPECT_EQ(stream_after_purge.CreatedAt(), stream_with_messages.CreatedAt());
    EXPECT_EQ(stream_after_purge.Name(), stream_with_messages.Name());
    EXPECT_EQ(stream_after_purge.TopicsCount(), stream_with_messages.TopicsCount());
    ASSERT_EQ(stream_after_purge.Topics().size(), stream_with_messages.Topics().size());

    for (const auto &topic : stream_after_purge.Topics()) {
        const std::string topic_name = topic.Name();
        const auto metadata_it       = topics_before_purge.find(topic_name);
        ASSERT_NE(metadata_it, topics_before_purge.end());
        const auto &metadata = metadata_it->second;
        EXPECT_EQ(topic.Id(), metadata.id);
        EXPECT_EQ(topic.CreatedAt(), metadata.created_at);
        EXPECT_EQ(topic.Name(), metadata.name);
        EXPECT_EQ(topic.MessageExpiry(), metadata.message_expiry);
        EXPECT_EQ(topic.CompressionAlgorithm(), metadata.compression_algorithm);
        EXPECT_EQ(topic.MaxTopicSize(), metadata.max_topic_size);
        EXPECT_EQ(topic.PartitionsCount(), metadata.partitions_count);
    }
}

TEST_F(E2E_Stream, PurgeStreamRemovesMessagesAndPreservesTopics) {
    RecordProperty("description", "Purges all stream messages while keeping the stream and topics intact.");
    const std::string stream_name       = GetRandomName();
    const std::string first_topic_name  = GetRandomName();
    const std::string second_topic_name = GetRandomName();
    auto client                         = GetLoggedInHighLevelClient();
    iggy::ffi::Client *ffi_client       = GetLoggedInClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), first_topic_name, 1));
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), second_topic_name, 1));

    const auto created_stream = client.GetStream(iggy::Identifier::String(stream_name));
    ASSERT_EQ(created_stream.Topics().size(), 2u);

    std::uint32_t first_topic_id  = 0;
    std::uint32_t second_topic_id = 0;
    bool first_topic_found        = false;
    bool second_topic_found       = false;
    for (const auto &topic : created_stream.Topics()) {
        const std::string topic_name = topic.Name();
        if (topic_name == first_topic_name) {
            first_topic_id    = topic.Id();
            first_topic_found = true;
        } else if (topic_name == second_topic_name) {
            second_topic_id    = topic.Id();
            second_topic_found = true;
        }
    }
    ASSERT_TRUE(first_topic_found);
    ASSERT_TRUE(second_topic_found);

    rust::Vec<iggy::ffi::IggyMessageToSend> first_topic_messages;
    for (std::uint32_t i = 0; i < 3; ++i) {
        first_topic_messages.push_back(iggy::ffi::make_message(to_payload("purge-stream-first-" + std::to_string(i)),
                                                               rust::Vec<iggy::ffi::HeaderEntry>()));
    }
    ASSERT_NO_THROW(ffi_client->send_messages(make_numeric_identifier(created_stream.Id()),
                                              make_numeric_identifier(first_topic_id), "partition_id",
                                              partition_id_bytes(0), std::move(first_topic_messages)));

    rust::Vec<iggy::ffi::IggyMessageToSend> second_topic_messages;
    for (std::uint32_t i = 0; i < 2; ++i) {
        second_topic_messages.push_back(iggy::ffi::make_message(to_payload("purge-stream-second-" + std::to_string(i)),
                                                                rust::Vec<iggy::ffi::HeaderEntry>()));
    }
    ASSERT_NO_THROW(ffi_client->send_messages(make_numeric_identifier(created_stream.Id()),
                                              make_numeric_identifier(second_topic_id), "partition_id",
                                              partition_id_bytes(0), std::move(second_topic_messages)));

    const auto stream_before_purge = client.GetStream(iggy::Identifier::String(stream_name));
    EXPECT_EQ(stream_before_purge.TopicsCount(), 2u);
    EXPECT_EQ(stream_before_purge.MessagesCount(), 5u);
    EXPECT_GT(stream_before_purge.SizeBytes(), 0u);

    std::unordered_map<std::string, std::uint64_t> messages_before_purge;
    for (const auto &topic : stream_before_purge.Topics()) {
        messages_before_purge[topic.Name()] = topic.MessagesCount();
    }
    EXPECT_EQ(messages_before_purge[first_topic_name], 3u);
    EXPECT_EQ(messages_before_purge[second_topic_name], 2u);

    const auto streams_before_purge = client.GetStreams();
    bool found_stream_before_purge  = false;
    for (const auto &stream : streams_before_purge) {
        if (stream.Name() == stream_name) {
            found_stream_before_purge = true;
            EXPECT_EQ(stream.TopicsCount(), 2u);
            EXPECT_EQ(stream.MessagesCount(), 5u);
            EXPECT_GT(stream.SizeBytes(), 0u);
            break;
        }
    }
    ASSERT_TRUE(found_stream_before_purge);

    ASSERT_NO_THROW(client.PurgeStream(iggy::Identifier::String(stream_name)));

    const auto stream_after_purge = client.GetStream(iggy::Identifier::String(stream_name));
    EXPECT_EQ(stream_after_purge.TopicsCount(), 2u);
    EXPECT_EQ(stream_after_purge.MessagesCount(), 0u);
    EXPECT_EQ(stream_after_purge.SizeBytes(), 0u);
    ASSERT_EQ(stream_after_purge.Topics().size(), 2u);
    for (const auto &topic : stream_after_purge.Topics()) {
        EXPECT_EQ(topic.MessagesCount(), 0u);
        EXPECT_EQ(topic.SizeBytes(), 0u);
    }

    const auto streams_after_purge = client.GetStreams();
    bool found_stream_after_purge  = false;
    for (const auto &stream : streams_after_purge) {
        if (stream.Name() == stream_name) {
            found_stream_after_purge = true;
            EXPECT_EQ(stream.TopicsCount(), 2u);
            EXPECT_EQ(stream.MessagesCount(), 0u);
            EXPECT_EQ(stream.SizeBytes(), 0u);
            break;
        }
    }
    ASSERT_TRUE(found_stream_after_purge);
}

TEST_F(E2E_Stream, PurgeStreamAcrossMultipleTopicsAndPartitionsClearsEverything) {
    RecordProperty("description", "Purges all messages across multiple topics and partitions in the stream.");
    const std::string stream_name       = GetRandomName();
    const std::string first_topic_name  = GetRandomName();
    const std::string second_topic_name = GetRandomName();
    auto client                         = GetLoggedInHighLevelClient();
    iggy::ffi::Client *ffi_client       = GetLoggedInClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), first_topic_name, 2));
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), second_topic_name, 3));

    const auto created_stream = client.GetStream(iggy::Identifier::String(stream_name));
    ASSERT_EQ(created_stream.Topics().size(), 2u);

    std::uint32_t first_topic_id  = 0;
    std::uint32_t second_topic_id = 0;
    bool first_topic_found        = false;
    bool second_topic_found       = false;
    for (const auto &topic : created_stream.Topics()) {
        const std::string topic_name = topic.Name();
        if (topic_name == first_topic_name) {
            first_topic_id    = topic.Id();
            first_topic_found = true;
        } else if (topic_name == second_topic_name) {
            second_topic_id    = topic.Id();
            second_topic_found = true;
        }
    }
    ASSERT_TRUE(first_topic_found);
    ASSERT_TRUE(second_topic_found);

    for (std::uint32_t partition_id = 0; partition_id < 2; ++partition_id) {
        rust::Vec<iggy::ffi::IggyMessageToSend> messages;
        for (std::uint32_t i = 0; i < 2; ++i) {
            messages.push_back(iggy::ffi::make_message(
                to_payload("purge-stream-topic-a-" + std::to_string(partition_id) + "-" + std::to_string(i)),
                rust::Vec<iggy::ffi::HeaderEntry>()));
        }
        ASSERT_NO_THROW(ffi_client->send_messages(make_numeric_identifier(created_stream.Id()),
                                                  make_numeric_identifier(first_topic_id), "partition_id",
                                                  partition_id_bytes(partition_id), std::move(messages)));
    }
    for (std::uint32_t partition_id = 0; partition_id < 3; ++partition_id) {
        rust::Vec<iggy::ffi::IggyMessageToSend> messages;
        for (std::uint32_t i = 0; i < 2; ++i) {
            messages.push_back(iggy::ffi::make_message(
                to_payload("purge-stream-topic-b-" + std::to_string(partition_id) + "-" + std::to_string(i)),
                rust::Vec<iggy::ffi::HeaderEntry>()));
        }
        ASSERT_NO_THROW(ffi_client->send_messages(make_numeric_identifier(created_stream.Id()),
                                                  make_numeric_identifier(second_topic_id), "partition_id",
                                                  partition_id_bytes(partition_id), std::move(messages)));
    }

    const auto stream_before_purge = client.GetStream(iggy::Identifier::String(stream_name));
    EXPECT_EQ(stream_before_purge.MessagesCount(), 10u);

    ASSERT_NO_THROW(client.PurgeStream(iggy::Identifier::String(stream_name)));

    const auto stream_after_purge = client.GetStream(iggy::Identifier::String(stream_name));
    EXPECT_EQ(stream_after_purge.TopicsCount(), 2u);
    EXPECT_EQ(stream_after_purge.MessagesCount(), 0u);
    EXPECT_EQ(stream_after_purge.SizeBytes(), 0u);
    for (const auto &topic : stream_after_purge.Topics()) {
        EXPECT_EQ(topic.MessagesCount(), 0u);
        EXPECT_EQ(topic.SizeBytes(), 0u);
    }
}

TEST_F(E2E_Stream, PurgeStreamThenSendMessagesAgainSucceeds) {
    RecordProperty("description", "Allows sending fresh messages to a topic after purging its parent stream.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();
    iggy::ffi::Client *ffi_client = GetLoggedInClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name, 1));

    const auto created_stream = client.GetStream(iggy::Identifier::String(stream_name));
    ASSERT_EQ(created_stream.Topics().size(), 1u);
    const std::uint32_t topic_id = created_stream.Topics().front().Id();

    rust::Vec<iggy::ffi::IggyMessageToSend> first_batch;
    first_batch.push_back(iggy::ffi::make_message(to_payload("before-purge"), rust::Vec<iggy::ffi::HeaderEntry>()));
    ASSERT_NO_THROW(ffi_client->send_messages(make_numeric_identifier(created_stream.Id()),
                                              make_numeric_identifier(topic_id), "partition_id", partition_id_bytes(0),
                                              std::move(first_batch)));

    ASSERT_NO_THROW(client.PurgeStream(iggy::Identifier::String(stream_name)));

    rust::Vec<iggy::ffi::IggyMessageToSend> second_batch;
    second_batch.push_back(iggy::ffi::make_message(to_payload("after-purge-0"), rust::Vec<iggy::ffi::HeaderEntry>()));
    second_batch.push_back(iggy::ffi::make_message(to_payload("after-purge-1"), rust::Vec<iggy::ffi::HeaderEntry>()));
    ASSERT_NO_THROW(ffi_client->send_messages(make_numeric_identifier(created_stream.Id()),
                                              make_numeric_identifier(topic_id), "partition_id", partition_id_bytes(0),
                                              std::move(second_batch)));

    const auto stream_after_resend = client.GetStream(iggy::Identifier::String(stream_name));
    EXPECT_EQ(stream_after_resend.TopicsCount(), 1u);
    EXPECT_EQ(stream_after_resend.MessagesCount(), 2u);
    EXPECT_GT(stream_after_resend.SizeBytes(), 0u);
    ASSERT_EQ(stream_after_resend.Topics().size(), 1u);
    EXPECT_EQ(stream_after_resend.Topics().front().MessagesCount(), 2u);
}

TEST_F(E2E_Stream, PurgeStreamTwiceKeepsStreamEmptyAndTopicsIntact) {
    RecordProperty("description", "Allows purging the same stream twice and keeps the stream empty after both calls.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();
    iggy::ffi::Client *ffi_client = GetLoggedInClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name, 1));

    const auto created_stream = client.GetStream(iggy::Identifier::String(stream_name));
    ASSERT_EQ(created_stream.Topics().size(), 1u);
    const std::uint32_t topic_id = created_stream.Topics().front().Id();

    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    for (std::uint32_t i = 0; i < 3; ++i) {
        messages.push_back(iggy::ffi::make_message(to_payload("purge-stream-twice-" + std::to_string(i)),
                                                   rust::Vec<iggy::ffi::HeaderEntry>()));
    }
    ASSERT_NO_THROW(ffi_client->send_messages(make_numeric_identifier(created_stream.Id()),
                                              make_numeric_identifier(topic_id), "partition_id", partition_id_bytes(0),
                                              std::move(messages)));

    ASSERT_NO_THROW(client.PurgeStream(iggy::Identifier::String(stream_name)));
    const auto stream_after_first_purge = client.GetStream(iggy::Identifier::String(stream_name));
    EXPECT_EQ(stream_after_first_purge.TopicsCount(), 1u);
    EXPECT_EQ(stream_after_first_purge.MessagesCount(), 0u);
    EXPECT_EQ(stream_after_first_purge.SizeBytes(), 0u);
    ASSERT_EQ(stream_after_first_purge.Topics().size(), 1u);
    EXPECT_EQ(stream_after_first_purge.Topics().front().MessagesCount(), 0u);
    EXPECT_EQ(stream_after_first_purge.Topics().front().SizeBytes(), 0u);

    ASSERT_NO_THROW(client.PurgeStream(iggy::Identifier::String(stream_name)));
    const auto stream_after_second_purge = client.GetStream(iggy::Identifier::String(stream_name));
    EXPECT_EQ(stream_after_second_purge.TopicsCount(), 1u);
    EXPECT_EQ(stream_after_second_purge.MessagesCount(), 0u);
    EXPECT_EQ(stream_after_second_purge.SizeBytes(), 0u);
    ASSERT_EQ(stream_after_second_purge.Topics().size(), 1u);
    EXPECT_EQ(stream_after_second_purge.Topics().front().MessagesCount(), 0u);
    EXPECT_EQ(stream_after_second_purge.Topics().front().SizeBytes(), 0u);
}

TEST_F(E2E_Stream, PurgeStreamBeforeLoginThrows) {
    RecordProperty("description", "Throws when stream purge is attempted before authentication.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);

    auto unauthenticated_client = GetLoggedOutHighLevelClient();

    ASSERT_THROW(unauthenticated_client.PurgeStream(iggy::Identifier::String(stream_name)), std::exception);
    ASSERT_NO_THROW(unauthenticated_client.Connect());
    ASSERT_THROW(unauthenticated_client.PurgeStream(iggy::Identifier::String(stream_name)), std::exception);
    ASSERT_NO_THROW(unauthenticated_client.Login("iggy", "iggy"));
    ASSERT_NO_THROW(unauthenticated_client.Disconnect());
    ASSERT_THROW(unauthenticated_client.PurgeStream(iggy::Identifier::String(stream_name)), std::exception);
}
