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

#include <cstdint>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "lib.rs.h"
#include "tests/common/test_helpers.hpp"

TEST(LowLevelE2E_Message, GetStreamsReturnsEmptyAfterCleanup) {
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

TEST(LowLevelE2E_Message, GetStreamsReturnsStreamAfterCreation) {
    RecordProperty("description", "Verifies created stream appears in get_streams result.");
    const std::string stream_name = "cpp-msg-get-streams";
    iggy::ffi::Client *client     = login_to_server();
    ASSERT_NE(client, nullptr);

    client->create_stream(stream_name);
    auto streams = client->get_streams();
    ASSERT_GE(streams.size(), 1);

    bool found = false;
    for (const auto &s : streams) {
        if (std::string(s.name) == stream_name) {
            found = true;
            break;
        }
    }
    ASSERT_TRUE(found) << "Stream '" << stream_name << "' not found in get_streams result";

    client->delete_stream(make_string_identifier(stream_name));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_Message, SendAndPollMessagesRoundTrip) {
    RecordProperty("description", "Sends 10 messages and polls them back, verifying count, offsets, and payloads.");
    const std::string stream_name = "cpp-msg-roundtrip";
    iggy::ffi::Client *client     = login_to_server();
    ASSERT_NE(client, nullptr);

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    client->create_topic(make_numeric_identifier(stream.id), "test-topic", 1, "none", 0, "never_expire", 0,
                         "server_default");

    rust::Vec<iggy::ffi::Message> messages;
    for (std::uint32_t i = 0; i < 10; i++) {
        iggy::ffi::Message msg;
        msg.new_message(to_payload("test message " + std::to_string(i)));
        messages.push_back(std::move(msg));
    }

    ASSERT_NO_THROW(client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0),
                                          "partition_id", partition_id_bytes(0), std::move(messages)));

    auto polled = client->poll_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), 0, "consumer",
                                        make_numeric_identifier(1), "offset", 0, 100, false);

    ASSERT_EQ(polled.count, 10u);
    ASSERT_EQ(polled.messages.size(), 10u);
    for (std::uint32_t i = 0; i < 10; i++) {
        ASSERT_EQ(polled.messages[i].offset, static_cast<std::uint64_t>(i));
        std::string expected = "test message " + std::to_string(i);
        std::string actual(polled.messages[i].payload.begin(), polled.messages[i].payload.end());
        ASSERT_EQ(actual, expected) << "Payload mismatch at offset " << i;
    }

    client->delete_stream(make_numeric_identifier(stream.id));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_Message, PollMessagesVerifyMessageIds) {
    RecordProperty("description", "Verifies that polled message IDs match the sent IDs.");
    const std::string stream_name = "cpp-msg-verify-ids";
    iggy::ffi::Client *client     = login_to_server();
    ASSERT_NE(client, nullptr);

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    client->create_topic(make_numeric_identifier(stream.id), "test-topic", 1, "none", 0, "never_expire", 0,
                         "server_default");

    rust::Vec<iggy::ffi::Message> messages;
    iggy::ffi::Message msg;
    msg.new_message(to_payload("id-test-message"));
    msg.id_lo = 42;
    msg.id_hi = 0;
    messages.push_back(std::move(msg));

    client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), "partition_id",
                          partition_id_bytes(0), std::move(messages));

    auto polled = client->poll_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), 0, "consumer",
                                        make_numeric_identifier(1), "offset", 0, 100, false);

    ASSERT_EQ(polled.messages.size(), 1u);
    ASSERT_EQ(polled.messages[0].id_lo, 42u);
    ASSERT_EQ(polled.messages[0].id_hi, 0u);

    client->delete_stream(make_numeric_identifier(stream.id));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_Message, PollMessagesFromEmptyPartition) {
    RecordProperty("description", "Verifies polling from an empty partition returns zero messages.");
    const std::string stream_name = "cpp-msg-empty-poll";
    iggy::ffi::Client *client     = login_to_server();
    ASSERT_NE(client, nullptr);

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    client->create_topic(make_numeric_identifier(stream.id), "test-topic", 1, "none", 0, "never_expire", 0,
                         "server_default");

    auto polled = client->poll_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), 0, "consumer",
                                        make_numeric_identifier(1), "offset", 0, 100, false);

    ASSERT_EQ(polled.count, 0u);
    ASSERT_EQ(polled.messages.size(), 0u);

    client->delete_stream(make_numeric_identifier(stream.id));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_Message, SendMessagesBeforeLoginThrows) {
    RecordProperty("description", "Verifies send_messages throws when not authenticated.");
    iggy::ffi::Client *client = nullptr;
    ASSERT_NO_THROW({ client = iggy::ffi::new_connection(""); });
    ASSERT_NE(client, nullptr);
    ASSERT_NO_THROW(client->connect());

    rust::Vec<iggy::ffi::Message> messages;
    iggy::ffi::Message msg;
    msg.new_message(to_payload("should-fail"));
    messages.push_back(std::move(msg));

    ASSERT_THROW(client->send_messages(make_numeric_identifier(1), make_numeric_identifier(1), "partition_id",
                                       partition_id_bytes(0), std::move(messages)),
                 std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}
