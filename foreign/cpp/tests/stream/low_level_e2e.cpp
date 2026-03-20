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
