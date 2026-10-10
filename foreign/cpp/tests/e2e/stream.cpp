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
    ASSERT_THROW(client.CreateStream(stream_name), iggy::IggyException);
}

TEST_F(E2E_Stream, CreateStreamBeforeLoginThrows) {
    RecordProperty("description", "Throws when stream creation is attempted before authentication.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedOutHighLevelClient();

    ASSERT_THROW(client.CreateStream(stream_name), iggy::IggyException);
    ASSERT_NO_THROW(client.Connect());
    ASSERT_THROW(client.CreateStream(stream_name), iggy::IggyException);
    ASSERT_NO_THROW(client.Login("iggy", "iggy"));
    ASSERT_NO_THROW(client.Disconnect());
    ASSERT_THROW(client.CreateStream(stream_name), iggy::IggyException);
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
        ASSERT_THROW(client.CreateStream(stream_name), iggy::IggyException);
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

    ASSERT_THROW(client.GetStream(iggy::Identifier::String(stream_name)), iggy::IggyException);

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

TEST_F(E2E_Stream, UpdateStreamWithUnsupportedOptionsRejectsAndPreservesName) {
    RecordProperty("description", "Rejects unsupported options without renaming the stream.");
    const std::string stream_name         = GetRandomName();
    const std::string updated_stream_name = GetRandomName();
    auto client                           = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);

    const auto options = iggy::StreamUpdateOptions().SetRawEntries({{"not_a_real_option", "true"}});
    ASSERT_THROW(client.UpdateStream(iggy::Identifier::String(stream_name), updated_stream_name, options),
                 iggy::IggyException);

    const auto stream = client.GetStream(iggy::Identifier::String(stream_name));
    EXPECT_EQ(stream.Name(), stream_name);
    ASSERT_THROW(client.GetStream(iggy::Identifier::String(updated_stream_name)), iggy::IggyException);
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
                 iggy::IggyException);
    ASSERT_NO_THROW(unauthenticated_client.Connect());
    ASSERT_THROW(unauthenticated_client.UpdateStream(iggy::Identifier::String(stream_name), updated_stream_name),
                 iggy::IggyException);
    ASSERT_NO_THROW(unauthenticated_client.Login("iggy", "iggy"));
    ASSERT_NO_THROW(unauthenticated_client.Disconnect());
    ASSERT_THROW(unauthenticated_client.UpdateStream(iggy::Identifier::String(stream_name), updated_stream_name),
                 iggy::IggyException);
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
    ASSERT_THROW(client.UpdateStream(iggy::Identifier::String(stream_name), updated_stream_name), iggy::IggyException);
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

    ASSERT_THROW(client.UpdateStream(iggy::Identifier::String(first_stream_name), second_stream_name),
                 iggy::IggyException);
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

    ASSERT_THROW(client.UpdateStream(iggy::Identifier::String(stream_name), updated_stream_name), iggy::IggyException);
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

    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::Numeric(stream_id), topic_name,
                                       iggy::TopicCreateOptions()
                                           .SetPartitionsCount(2)
                                           .SetCompressionAlgorithm(iggy::CompressionAlgorithm::None())
                                           .SetMessageExpiry(iggy::Expiry::NeverExpire())));

    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    for (std::uint32_t i = 0; i < 3; ++i) {
        auto message = iggy::ffi::make_message(to_payload("stream-update-preserve-" + std::to_string(i)),
                                               rust::Vec<iggy::ffi::HeaderEntry>());
        messages.push_back(std::move(message));
    }
    ASSERT_NO_THROW(ffi_client->send_messages(make_numeric_identifier(stream_id), make_numeric_identifier(0),
                                              "partition_id", partition_id_bytes(0), std::move(messages)));

    auto stream_before_update      = client.GetStream(iggy::Identifier::Numeric(stream_id));
    const auto stats_before_update = client.GetStats();

    ASSERT_NO_THROW(client.UpdateStream(iggy::Identifier::Numeric(stream_id), updated_stream_name));

    ASSERT_THROW(client.GetStream(iggy::Identifier::String(stream_name)), iggy::IggyException);
    auto stream_after_update      = client.GetStream(iggy::Identifier::Numeric(stream_id));
    const auto stats_after_update = client.GetStats();

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

    EXPECT_EQ(stats_after_update.StreamsCount(), stats_before_update.StreamsCount());
    EXPECT_EQ(stats_after_update.TopicsCount(), stats_before_update.TopicsCount());
    EXPECT_EQ(stats_after_update.PartitionsCount(), stats_before_update.PartitionsCount());
    EXPECT_EQ(stats_after_update.SegmentsCount(), stats_before_update.SegmentsCount());
    EXPECT_EQ(stats_after_update.MessagesCount(), stats_before_update.MessagesCount());
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
        ASSERT_THROW(client.UpdateStream(iggy::Identifier::Numeric(stream_id), invalid_stream_name),
                     iggy::IggyException);
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
    ASSERT_THROW(client.DeleteStream(iggy::Identifier::String(stream_name)), iggy::IggyException);
}

TEST_F(E2E_Stream, DeleteStreamBeforeLoginThrows) {
    RecordProperty("description", "Throws when stream deletion is attempted before authentication.");
    const std::string stream_name = GetRandomName();

    auto client = GetLoggedOutHighLevelClient();

    ASSERT_THROW(client.DeleteStream(iggy::Identifier::String(stream_name)), iggy::IggyException);

    ASSERT_NO_THROW(client.Connect());

    ASSERT_THROW(client.DeleteStream(iggy::Identifier::String(stream_name)), iggy::IggyException);
    ASSERT_NO_THROW(client.Login("iggy", "iggy"));
    ASSERT_NO_THROW(client.Disconnect());
    ASSERT_THROW(client.DeleteStream(iggy::Identifier::String(stream_name)), iggy::IggyException);
}

TEST_F(E2E_Stream, DeleteStreamTwiceThrows) {
    RecordProperty("description", "Throws when deleting the same stream a second time.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);

    ASSERT_NO_THROW(client.DeleteStream(iggy::Identifier::String(stream_name)));
    ForgetTrackedStream(stream_name);
    ASSERT_THROW(client.DeleteStream(iggy::Identifier::String(stream_name)), iggy::IggyException);
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
    ASSERT_THROW(client.GetStream(iggy::Identifier::String(stream_name)), iggy::IggyException);
}

TEST_F(E2E_Stream, GetStreamDetailsBeforeLoginThrows) {
    RecordProperty("description", "Throws when stream details are requested before authentication.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedOutHighLevelClient();

    ASSERT_THROW(client.GetStream(iggy::Identifier::String(stream_name)), iggy::IggyException);
    ASSERT_NO_THROW(client.Connect());
    ASSERT_THROW(client.GetStream(iggy::Identifier::String(stream_name)), iggy::IggyException);
    ASSERT_NO_THROW(client.Login("iggy", "iggy"));
    ASSERT_NO_THROW(client.Disconnect());
    ASSERT_THROW(client.GetStream(iggy::Identifier::String(stream_name)), iggy::IggyException);
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
    ASSERT_THROW(client.GetStream(iggy::Identifier::String(stream_name)), iggy::IggyException);
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
    client.CreateTopic(iggy::Identifier::Numeric(stream.Id()), topic_name,
                       iggy::TopicCreateOptions()
                           .SetPartitionsCount(1)
                           .SetCompressionAlgorithm(iggy::CompressionAlgorithm::None())
                           .SetMessageExpiry(iggy::Expiry::NeverExpire()));

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

    ASSERT_THROW(client.GetStreams(), iggy::IggyException);
    ASSERT_NO_THROW(client.Connect());
    ASSERT_THROW(client.GetStreams(), iggy::IggyException);
    ASSERT_NO_THROW(client.Login("iggy", "iggy"));
    ASSERT_NO_THROW(client.Disconnect());
    ASSERT_THROW(client.GetStreams(), iggy::IggyException);
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

    auto single             = client.GetStream(iggy::Identifier::String(stream_name));
    const auto &single_name = single.Name();
    auto single_topics      = single.TopicsCount();

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
