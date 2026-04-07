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

// TODO(slbotbm): create fixture for setup/teardown.

#include <cstdint>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "lib.rs.h"
#include "tests/common/test_helpers.hpp"

// TODO(slbotbm): Add tests for purge_stream after implementing send_messages(...).

TEST(LowLevelE2E_Stream, CreateStreamAfterLogin) {
    RecordProperty("description", "Creates a stream successfully after authenticating.");
    const std::string stream_name = "cpp-create-stream-after-login";
    iggy::ffi::Client *client     = login_to_server();
    ASSERT_NE(client, nullptr);
    ASSERT_NO_THROW(client->create_stream(stream_name));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Stream, CreateDuplicateStreamThrows) {
    RecordProperty("description", "Rejects creating the same stream twice.");
    const std::string stream_name = "cpp-create-stream-duplicate";
    iggy::ffi::Client *client     = login_to_server();
    ASSERT_NE(client, nullptr);
    ASSERT_NO_THROW(client->create_stream(stream_name));
    ASSERT_THROW(client->create_stream(stream_name), std::exception);
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Stream, CreateStreamBeforeLoginThrows) {
    RecordProperty("description", "Throws when stream creation is attempted before authentication.");
    const std::string stream_name = "cpp-create-stream-immediate-new-connection";
    iggy::ffi::Client *client     = nullptr;
    ASSERT_NO_THROW({ client = iggy::ffi::new_connection(""); });
    ASSERT_NE(client, nullptr);

    ASSERT_THROW(client->create_stream(stream_name), std::exception);
    ASSERT_NO_THROW(client->connect());
    ASSERT_THROW(client->create_stream(stream_name), std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Stream, CreateStreamValidatesNameConstraintsAndUniqueness) {
    RecordProperty("description",
                   "Validates stream name length constraints and accepts the maximum allowed name length.");
    iggy::ffi::Client *client = login_to_server();
    ASSERT_NE(client, nullptr);

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

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Stream, CreateStreamWithEmojiName) {
    RecordProperty("description", "Creates a stream with a UTF-8 emoji name.");
    const std::string stream_name = "🚀🚀🚀🚀Apache Iggy🚀🚀🚀🚀";
    iggy::ffi::Client *client     = login_to_server();
    ASSERT_NE(client, nullptr);

    ASSERT_NO_THROW(client->create_stream(stream_name));
    ASSERT_NO_THROW({
        const auto stream_details              = client->get_stream(make_string_identifier(stream_name));
        const std::string returned_stream_name = static_cast<std::string>(stream_details.name);
        EXPECT_NE(stream_details.id, 0u);
        EXPECT_EQ(returned_stream_name, stream_name);
        EXPECT_EQ(stream_details.topics_count, 0u);
        EXPECT_EQ(stream_details.topics.size(), 0u);
    });

    ASSERT_NO_THROW(client->delete_stream(make_string_identifier(stream_name)));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Stream, StreamCreatedAndDeletedSuccessfully) {
    RecordProperty("description", "Creates a stream and deletes it successfully by string identifier.");
    const std::string stream_name = "cpp-delete-stream-created-and-deleted";
    iggy::ffi::Client *client     = login_to_server();
    ASSERT_NE(client, nullptr);
    ASSERT_NO_THROW(client->create_stream(stream_name));

    ASSERT_NO_THROW(client->delete_stream(make_string_identifier(stream_name)));

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Stream, DeleteNotCreatedStreamThrows) {
    RecordProperty("description", "Throws when deleting a stream that does not exist.");
    const std::string stream_name = "cpp-delete-stream-not-created";
    iggy::ffi::Client *client     = login_to_server();
    ASSERT_NE(client, nullptr);

    ASSERT_THROW(client->delete_stream(make_string_identifier(stream_name)), std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Stream, DeleteStreamBeforeLoginThrows) {
    RecordProperty("description", "Throws when stream deletion is attempted before authentication.");
    const std::string stream_name = "cpp-delete-stream-without-login";

    iggy::ffi::Client *client = nullptr;
    ASSERT_NO_THROW({ client = iggy::ffi::new_connection(""); });
    ASSERT_NE(client, nullptr);

    ASSERT_THROW(client->delete_stream(make_string_identifier(stream_name)), std::exception);

    ASSERT_NO_THROW(client->connect());

    ASSERT_THROW(client->delete_stream(make_string_identifier(stream_name)), std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Stream, DeleteStreamTwiceThrows) {
    RecordProperty("description", "Throws when deleting the same stream a second time.");
    const std::string stream_name = "cpp-delete-stream-twice";
    iggy::ffi::Client *client     = login_to_server();
    ASSERT_NE(client, nullptr);
    ASSERT_NO_THROW(client->create_stream(stream_name));

    ASSERT_NO_THROW(client->delete_stream(make_string_identifier(stream_name)));
    ASSERT_THROW(client->delete_stream(make_string_identifier(stream_name)), std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Stream, DeleteStreamWithInvalidIdentifierThrows) {
    RecordProperty("description", "Rejects stream deletion requests that use invalid identifier formats.");
    iggy::ffi::Client *client = login_to_server();
    ASSERT_NE(client, nullptr);

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

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Stream, GetStreamDetailsWithInvalidIdentifierThrows) {
    RecordProperty("description", "Rejects stream detail lookups that use invalid identifier formats.");
    iggy::ffi::Client *client = login_to_server();
    ASSERT_NE(client, nullptr);

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

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Stream, GetStreamByStringIdentifierReturnsStreamDetails) {
    RecordProperty("description", "Returns expected stream details when looked up by string identifier.");
    const std::string stream_name = "cpp-get-stream-by-string";
    iggy::ffi::Client *client     = login_to_server();
    ASSERT_NE(client, nullptr);
    ASSERT_NO_THROW(client->create_stream(stream_name));

    ASSERT_NO_THROW({
        const auto stream_details = client->get_stream(make_string_identifier(stream_name));
        EXPECT_NE(stream_details.id, 0u);
        EXPECT_EQ(stream_details.name, stream_name);
        EXPECT_EQ(stream_details.topics_count, 0u);
        EXPECT_EQ(stream_details.topics.size(), 0u);
        EXPECT_EQ(stream_details.messages_count, 0u);
        EXPECT_EQ(stream_details.size_bytes, 0u);
    });

    ASSERT_NO_THROW(client->delete_stream(make_string_identifier(stream_name)));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Stream, GetNonExistentStreamDetailsThrows) {
    RecordProperty("description", "Throws when requesting details for a stream that does not exist.");
    const std::string stream_name = "cpp-get-nonexistent-stream-details";
    iggy::ffi::Client *client     = login_to_server();
    ASSERT_NE(client, nullptr);
    ASSERT_THROW(client->get_stream(make_string_identifier(stream_name)), std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Stream, GetStreamDetailsBeforeLoginThrows) {
    RecordProperty("description", "Throws when stream details are requested before authentication.");
    const std::string stream_name = "cpp-get-stream-details-without-login";
    iggy::ffi::Client *client     = nullptr;
    ASSERT_NO_THROW({ client = iggy::ffi::new_connection(""); });
    ASSERT_NE(client, nullptr);

    ASSERT_THROW(client->get_stream(make_string_identifier(stream_name)), std::exception);
    ASSERT_NO_THROW(client->connect());
    ASSERT_THROW(client->get_stream(make_string_identifier(stream_name)), std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Stream, GetDeletedStreamDetailsThrows) {
    RecordProperty("description", "Throws when requesting details for a stream after it has been deleted.");
    const std::string stream_name = "cpp-get-deleted-stream-details";
    iggy::ffi::Client *client     = login_to_server();
    ASSERT_NE(client, nullptr);
    ASSERT_NO_THROW(client->create_stream(stream_name));
    ASSERT_NO_THROW(client->get_stream(make_string_identifier(stream_name)));
    ASSERT_NO_THROW(client->delete_stream(make_string_identifier(stream_name)));
    ASSERT_THROW(client->get_stream(make_string_identifier(stream_name)), std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Stream, GetStreamByNumericIdentifierReturnsStreamDetails) {
    RecordProperty("description", "Returns expected stream details when looked up by numeric identifier.");
    const std::string stream_name = "cpp-get-stream-by-numeric";
    iggy::ffi::Client *client     = login_to_server();
    ASSERT_NE(client, nullptr);
    ASSERT_NO_THROW(client->create_stream(stream_name));

    std::uint32_t stream_id = 0;
    ASSERT_NO_THROW({
        const auto by_string = client->get_stream(make_string_identifier(stream_name));
        stream_id            = by_string.id;
        EXPECT_NE(stream_id, 0u);
    });

    ASSERT_NO_THROW({
        const auto by_numeric = client->get_stream(make_numeric_identifier(stream_id));
        EXPECT_EQ(by_numeric.id, stream_id);
        EXPECT_EQ(by_numeric.name, stream_name);
        EXPECT_EQ(by_numeric.topics_count, 0u);
        EXPECT_EQ(by_numeric.topics.size(), 0u);
    });

    ASSERT_NO_THROW(client->delete_stream(make_string_identifier(stream_name)));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Stream, GetStreamsReturnsEmptyAfterCleanup) {
    RecordProperty("description", "Verifies get_streams returns empty vector after cleaning up all streams.");
    iggy::ffi::Client *client = login_to_server();
    ASSERT_NE(client, nullptr);

    auto streams = client->get_streams();
    for (const auto &s : streams) {
        client->delete_stream(make_numeric_identifier(s.id));
    }

    streams = client->get_streams();
    ASSERT_EQ(streams.size(), 0);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_Stream, GetStreamsReturnsStreamAfterCreation) {
    RecordProperty("description", "Verifies created stream appears in get_streams result.");
    const std::string stream_name = "cpp-stream-get-streams";
    iggy::ffi::Client *client     = login_to_server();
    ASSERT_NE(client, nullptr);

    client->create_stream(stream_name);
    auto streams = client->get_streams();
    ASSERT_GE(streams.size(), 1);

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

    client->delete_stream(make_string_identifier(stream_name));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_Stream, GetStreamsFieldsVerification) {
    RecordProperty("description",
                   "Verifies get_streams returns correct field values after creating stream with topic and messages.");
    const std::string stream_name = "cpp-stream-fields-verify";
    iggy::ffi::Client *client     = login_to_server();
    ASSERT_NE(client, nullptr);

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    client->create_topic(make_numeric_identifier(stream.id), "test-topic", 1, "none", 0, "never_expire", 0,
                         "server_default");

    rust::Vec<iggy::ffi::Message> messages;
    for (std::uint32_t i = 0; i < 5; i++) {
        iggy::ffi::Message msg;
        msg.new_message(to_payload("field-verify-message-" + std::to_string(i)));
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

    client->delete_stream(make_numeric_identifier(stream.id));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_Stream, GetStreamsBeforeLoginThrows) {
    RecordProperty("description", "Throws when get_streams is called before authentication.");
    iggy::ffi::Client *client = nullptr;
    ASSERT_NO_THROW({ client = iggy::ffi::new_connection(""); });
    ASSERT_NE(client, nullptr);

    ASSERT_THROW(client->get_streams(), std::exception);
    ASSERT_NO_THROW(client->connect());
    ASSERT_THROW(client->get_streams(), std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Stream, GetStreamsConsistentWithGetStream) {
    RecordProperty("description", "Verifies get_streams result is consistent with get_stream for the same stream.");
    const std::string stream_name = "cpp-stream-consistency";
    iggy::ffi::Client *client     = login_to_server();
    ASSERT_NE(client, nullptr);

    client->create_stream(stream_name);

    std::string list_name;
    std::uint32_t list_topics_count = 0;
    std::uint64_t list_created_at   = 0;
    std::uint64_t list_size_bytes   = 0;
    auto streams                    = client->get_streams();
    for (const auto &s : streams) {
        if (std::string(s.name) == stream_name) {
            list_name         = std::string(s.name);
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
    EXPECT_EQ(list_topics_count, single_topics);
    EXPECT_EQ(list_created_at, single.created_at);
    EXPECT_EQ(list_size_bytes, single.size_bytes);

    client->delete_stream(make_string_identifier(stream_name));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}

TEST(LowLevelE2E_Stream, GetStreamsRepeatedCallsReturnSameResult) {
    RecordProperty("description", "Verifies repeated get_streams calls return consistent results.");
    const std::string stream_name = "cpp-stream-repeated";
    iggy::ffi::Client *client     = login_to_server();
    ASSERT_NE(client, nullptr);

    client->create_stream(stream_name);

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

    client->delete_stream(make_string_identifier(stream_name));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
    client = nullptr;
}
