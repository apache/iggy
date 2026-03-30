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

#include <gtest/gtest.h>

#include "lib.rs.h"
#include "tests/common/test_helpers.hpp"

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

TEST(LowLevelE2E_Message, SendMessagesWithInvalidStreamId) {
    RecordProperty("description", "Throws when sending messages with an invalid stream identifier.");
    iggy::ffi::Client *client = login_to_server();
    ASSERT_NE(client, nullptr);

    rust::Vec<iggy::ffi::Message> messages;
    iggy::ffi::Message msg;
    msg.new_message(to_payload("test"));
    messages.push_back(std::move(msg));

    iggy::ffi::Identifier invalid_id;
    invalid_id.kind   = "invalid";
    invalid_id.length = 0;

    ASSERT_THROW(client->send_messages(invalid_id, make_numeric_identifier(1), "partition_id", partition_id_bytes(0),
                                       std::move(messages)),
                 std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_Message, SendMessagesToNonExistentStream) {
    RecordProperty("description", "Throws when sending messages to a non-existent stream.");
    iggy::ffi::Client *client = login_to_server();
    ASSERT_NE(client, nullptr);

    rust::Vec<iggy::ffi::Message> messages;
    iggy::ffi::Message msg;
    msg.new_message(to_payload("test"));
    messages.push_back(std::move(msg));

    ASSERT_THROW(client->send_messages(make_string_identifier("nonexistent-stream-12345"), make_numeric_identifier(0),
                                       "partition_id", partition_id_bytes(0), std::move(messages)),
                 std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_Message, SendMessagesWithInvalidPartitioningKind) {
    RecordProperty("description", "Throws when sending messages with an invalid partitioning kind.");
    const std::string stream_name = "cpp-msg-invalid-part-kind";
    iggy::ffi::Client *client     = login_to_server();
    ASSERT_NE(client, nullptr);

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    client->create_topic(make_numeric_identifier(stream.id), "test-topic", 1, "none", 0, "never_expire", 0,
                         "server_default");

    rust::Vec<iggy::ffi::Message> messages;
    iggy::ffi::Message msg;
    msg.new_message(to_payload("test"));
    messages.push_back(std::move(msg));

    ASSERT_THROW(client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), "invalid_kind",
                                       partition_id_bytes(0), std::move(messages)),
                 std::exception);

    client->delete_stream(make_numeric_identifier(stream.id));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_Message, SendMessagesWithInvalidPartitioningValue) {
    RecordProperty("description", "Throws when sending messages with insufficient partitioning value bytes.");
    const std::string stream_name = "cpp-msg-invalid-part-val";
    iggy::ffi::Client *client     = login_to_server();
    ASSERT_NE(client, nullptr);

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    client->create_topic(make_numeric_identifier(stream.id), "test-topic", 1, "none", 0, "never_expire", 0,
                         "server_default");

    rust::Vec<iggy::ffi::Message> messages;
    iggy::ffi::Message msg;
    msg.new_message(to_payload("test"));
    messages.push_back(std::move(msg));

    rust::Vec<std::uint8_t> short_bytes;
    short_bytes.push_back(0x00);
    short_bytes.push_back(0x01);

    ASSERT_THROW(client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), "partition_id",
                                       std::move(short_bytes), std::move(messages)),
                 std::exception);

    client->delete_stream(make_numeric_identifier(stream.id));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_Message, SendMessagesToSpecificPartitionVerified) {
    RecordProperty("description",
                   "Verifies messages sent to a specific partition are only retrievable from that partition.");
    const std::string stream_name = "cpp-msg-specific-part";
    iggy::ffi::Client *client     = login_to_server();
    ASSERT_NE(client, nullptr);

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    client->create_topic(make_numeric_identifier(stream.id), "test-topic", 3, "none", 0, "never_expire", 0,
                         "server_default");

    rust::Vec<iggy::ffi::Message> messages;
    for (std::uint32_t i = 0; i < 5; i++) {
        iggy::ffi::Message msg;
        msg.new_message(to_payload("partition-test-" + std::to_string(i)));
        messages.push_back(std::move(msg));
    }

    client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), "partition_id",
                          partition_id_bytes(0), std::move(messages));

    auto polled_part0 = client->poll_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), 0,
                                              "consumer", make_numeric_identifier(1), "offset", 0, 100, false);
    ASSERT_EQ(polled_part0.count, 5u);

    auto polled_part1 = client->poll_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), 1,
                                              "consumer", make_numeric_identifier(1), "offset", 0, 100, false);
    ASSERT_EQ(polled_part1.count, 0u);

    client->delete_stream(make_numeric_identifier(stream.id));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_Message, SendEmptyMessageVector) {
    RecordProperty("description", "Verifies behavior when sending an empty message vector.");
    const std::string stream_name = "cpp-msg-empty-vec";
    iggy::ffi::Client *client     = login_to_server();
    ASSERT_NE(client, nullptr);

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    client->create_topic(make_numeric_identifier(stream.id), "test-topic", 1, "none", 0, "never_expire", 0,
                         "server_default");

    rust::Vec<iggy::ffi::Message> empty_messages;

    ASSERT_THROW(client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), "partition_id",
                                       partition_id_bytes(0), std::move(empty_messages)),
                 std::exception);

    client->delete_stream(make_numeric_identifier(stream.id));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_Message, SendMessageWithEmptyPayload) {
    RecordProperty("description", "Throws when sending a message with an empty payload.");
    const std::string stream_name = "cpp-msg-empty-payload";
    iggy::ffi::Client *client     = login_to_server();
    ASSERT_NE(client, nullptr);

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    client->create_topic(make_numeric_identifier(stream.id), "test-topic", 1, "none", 0, "never_expire", 0,
                         "server_default");

    rust::Vec<iggy::ffi::Message> messages;
    iggy::ffi::Message msg;
    rust::Vec<std::uint8_t> empty_payload;
    msg.new_message(std::move(empty_payload));
    messages.push_back(std::move(msg));

    ASSERT_THROW(client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), "partition_id",
                                       partition_id_bytes(0), std::move(messages)),
                 std::exception);

    client->delete_stream(make_numeric_identifier(stream.id));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_Message, SendMessageWithOversizedPayload) {
    RecordProperty("description", "Throws when sending a message exceeding maximum payload size.");
    const std::string stream_name = "cpp-msg-oversized";
    iggy::ffi::Client *client     = login_to_server();
    ASSERT_NE(client, nullptr);

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    client->create_topic(make_numeric_identifier(stream.id), "test-topic", 1, "none", 0, "never_expire", 0,
                         "server_default");

    rust::Vec<std::uint8_t> oversized_payload;
    for (std::uint32_t i = 0; i < 64000001u; i++) {
        oversized_payload.push_back(0x41);
    }

    rust::Vec<iggy::ffi::Message> messages;
    iggy::ffi::Message msg;
    msg.new_message(std::move(oversized_payload));
    messages.push_back(std::move(msg));

    ASSERT_THROW(client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), "partition_id",
                                       partition_id_bytes(0), std::move(messages)),
                 std::exception);

    client->delete_stream(make_numeric_identifier(stream.id));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_Message, SendMessagesPreservesOrder) {
    RecordProperty("description", "Verifies messages are stored and retrieved in the order they were sent.");
    const std::string stream_name = "cpp-msg-order";
    iggy::ffi::Client *client     = login_to_server();
    ASSERT_NE(client, nullptr);

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    client->create_topic(make_numeric_identifier(stream.id), "test-topic", 1, "none", 0, "never_expire", 0,
                         "server_default");

    rust::Vec<iggy::ffi::Message> messages;
    for (std::uint32_t i = 0; i < 50; i++) {
        iggy::ffi::Message msg;
        msg.new_message(to_payload("order-" + std::to_string(i)));
        messages.push_back(std::move(msg));
    }

    client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), "partition_id",
                          partition_id_bytes(0), std::move(messages));

    auto polled = client->poll_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), 0, "consumer",
                                        make_numeric_identifier(1), "offset", 0, 100, false);

    ASSERT_EQ(polled.count, 50u);
    for (std::uint32_t i = 0; i < 50; i++) {
        ASSERT_EQ(polled.messages[i].offset, static_cast<std::uint64_t>(i));
        std::string expected = "order-" + std::to_string(i);
        std::string actual(polled.messages[i].payload.begin(), polled.messages[i].payload.end());
        EXPECT_EQ(actual, expected) << "Payload mismatch at offset " << i;
    }

    client->delete_stream(make_numeric_identifier(stream.id));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_Message, SendMessagesWithDuplicateIds) {
    RecordProperty("description", "Verifies sending multiple messages with the same ID succeeds.");
    const std::string stream_name = "cpp-msg-dup-ids";
    iggy::ffi::Client *client     = login_to_server();
    ASSERT_NE(client, nullptr);

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    client->create_topic(make_numeric_identifier(stream.id), "test-topic", 1, "none", 0, "never_expire", 0,
                         "server_default");

    rust::Vec<iggy::ffi::Message> messages;
    for (std::uint32_t i = 0; i < 3; i++) {
        iggy::ffi::Message msg;
        msg.new_message(to_payload("dup-id-msg-" + std::to_string(i)));
        msg.id_lo = 99;
        msg.id_hi = 0;
        messages.push_back(std::move(msg));
    }

    ASSERT_NO_THROW(client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0),
                                          "partition_id", partition_id_bytes(0), std::move(messages)));

    auto polled = client->poll_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), 0, "consumer",
                                        make_numeric_identifier(1), "offset", 0, 100, false);

    ASSERT_EQ(polled.count, 3u);
    for (std::size_t i = 0; i < polled.messages.size(); i++) {
        EXPECT_EQ(polled.messages[i].id_lo, 99u);
    }

    client->delete_stream(make_numeric_identifier(stream.id));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_Message, SendMessagesWithVariousPayloads) {
    RecordProperty("description",
                   "Verifies various payload types including null bytes, UTF-8, and binary data are preserved.");
    const std::string stream_name = "cpp-msg-various-payloads";
    iggy::ffi::Client *client     = login_to_server();
    ASSERT_NE(client, nullptr);

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    client->create_topic(make_numeric_identifier(stream.id), "test-topic", 1, "none", 0, "never_expire", 0,
                         "server_default");

    rust::Vec<std::uint8_t> payload_null;
    payload_null.push_back(0x00);
    payload_null.push_back(0x01);
    payload_null.push_back(0x00);
    payload_null.push_back(0xFF);

    rust::Vec<std::uint8_t> payload_binary;
    payload_binary.push_back(0xDE);
    payload_binary.push_back(0xAD);
    payload_binary.push_back(0xBE);
    payload_binary.push_back(0xEF);

    rust::Vec<iggy::ffi::Message> messages;

    iggy::ffi::Message msg0;
    msg0.new_message(to_payload("simple ascii"));
    messages.push_back(std::move(msg0));

    iggy::ffi::Message msg1;
    msg1.new_message(std::move(payload_null));
    messages.push_back(std::move(msg1));

    iggy::ffi::Message msg2;
    msg2.new_message(to_payload("héllo wörld"));
    messages.push_back(std::move(msg2));

    iggy::ffi::Message msg3;
    msg3.new_message(std::move(payload_binary));
    messages.push_back(std::move(msg3));

    client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), "partition_id",
                          partition_id_bytes(0), std::move(messages));

    auto polled = client->poll_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), 0, "consumer",
                                        make_numeric_identifier(1), "offset", 0, 100, false);

    ASSERT_EQ(polled.count, 4u);

    std::string ascii_actual(polled.messages[0].payload.begin(), polled.messages[0].payload.end());
    EXPECT_EQ(ascii_actual, "simple ascii");

    ASSERT_EQ(polled.messages[1].payload.size(), 4u);
    EXPECT_EQ(polled.messages[1].payload[0], 0x00);
    EXPECT_EQ(polled.messages[1].payload[1], 0x01);
    EXPECT_EQ(polled.messages[1].payload[2], 0x00);
    EXPECT_EQ(polled.messages[1].payload[3], 0xFF);

    std::string utf8_actual(polled.messages[2].payload.begin(), polled.messages[2].payload.end());
    EXPECT_EQ(utf8_actual, "héllo wörld");

    ASSERT_EQ(polled.messages[3].payload.size(), 4u);
    EXPECT_EQ(polled.messages[3].payload[0], 0xDE);
    EXPECT_EQ(polled.messages[3].payload[1], 0xAD);
    EXPECT_EQ(polled.messages[3].payload[2], 0xBE);
    EXPECT_EQ(polled.messages[3].payload[3], 0xEF);

    client->delete_stream(make_numeric_identifier(stream.id));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_Message, PollMessagesBeforeLoginThrows) {
    RecordProperty("description", "Throws when polling messages before authentication.");
    iggy::ffi::Client *client = nullptr;
    ASSERT_NO_THROW({ client = iggy::ffi::new_connection(""); });
    ASSERT_NE(client, nullptr);
    ASSERT_NO_THROW(client->connect());

    ASSERT_THROW(client->poll_messages(make_numeric_identifier(1), make_numeric_identifier(0), 0, "consumer",
                                       make_numeric_identifier(1), "offset", 0, 10, false),
                 std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_Message, PollMessagesWithInvalidStreamId) {
    RecordProperty("description", "Throws when polling messages with an invalid stream identifier.");
    iggy::ffi::Client *client = login_to_server();
    ASSERT_NE(client, nullptr);

    iggy::ffi::Identifier invalid_id;
    invalid_id.kind   = "invalid";
    invalid_id.length = 0;

    ASSERT_THROW(client->poll_messages(invalid_id, make_numeric_identifier(0), 0, "consumer",
                                       make_numeric_identifier(1), "offset", 0, 10, false),
                 std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_Message, PollMessagesFromNonExistentStream) {
    RecordProperty("description", "Throws when polling messages from a non-existent stream.");
    iggy::ffi::Client *client = login_to_server();
    ASSERT_NE(client, nullptr);

    ASSERT_THROW(client->poll_messages(make_string_identifier("nonexistent-stream-poll"), make_numeric_identifier(0), 0,
                                       "consumer", make_numeric_identifier(1), "offset", 0, 10, false),
                 std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_Message, PollMessagesWithInvalidConsumerKind) {
    RecordProperty("description", "Throws when polling messages with an invalid consumer kind.");
    const std::string stream_name = "cpp-msg-invalid-consumer";
    iggy::ffi::Client *client     = login_to_server();
    ASSERT_NE(client, nullptr);

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    client->create_topic(make_numeric_identifier(stream.id), "test-topic", 1, "none", 0, "never_expire", 0,
                         "server_default");

    ASSERT_THROW(client->poll_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), 0, "invalid",
                                       make_numeric_identifier(1), "offset", 0, 10, false),
                 std::exception);

    client->delete_stream(make_numeric_identifier(stream.id));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_Message, PollMessagesWithInvalidStrategyKind) {
    RecordProperty("description", "Throws when polling messages with an invalid polling strategy kind.");
    const std::string stream_name = "cpp-msg-invalid-strategy";
    iggy::ffi::Client *client     = login_to_server();
    ASSERT_NE(client, nullptr);

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    client->create_topic(make_numeric_identifier(stream.id), "test-topic", 1, "none", 0, "never_expire", 0,
                         "server_default");

    ASSERT_THROW(client->poll_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), 0, "consumer",
                                       make_numeric_identifier(1), "invalid", 0, 10, false),
                 std::exception);

    client->delete_stream(make_numeric_identifier(stream.id));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_Message, PollMessagesCountLessThanAvailable) {
    RecordProperty("description", "Returns only the requested count when fewer messages are requested than available.");
    const std::string stream_name = "cpp-msg-count-less";
    iggy::ffi::Client *client     = login_to_server();
    ASSERT_NE(client, nullptr);

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    client->create_topic(make_numeric_identifier(stream.id), "test-topic", 1, "none", 0, "never_expire", 0,
                         "server_default");

    rust::Vec<iggy::ffi::Message> messages;
    for (std::uint32_t i = 0; i < 10; i++) {
        iggy::ffi::Message msg;
        msg.new_message(to_payload("msg-" + std::to_string(i)));
        messages.push_back(std::move(msg));
    }

    client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), "partition_id",
                          partition_id_bytes(0), std::move(messages));

    auto polled = client->poll_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), 0, "consumer",
                                        make_numeric_identifier(1), "offset", 0, 5, false);

    ASSERT_EQ(polled.count, 5u);
    ASSERT_EQ(polled.messages.size(), 5u);

    client->delete_stream(make_numeric_identifier(stream.id));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_Message, PollMessagesWithLargeOffset) {
    RecordProperty("description", "Returns zero messages when polling with an offset beyond available messages.");
    const std::string stream_name = "cpp-msg-large-offset";
    iggy::ffi::Client *client     = login_to_server();
    ASSERT_NE(client, nullptr);

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    client->create_topic(make_numeric_identifier(stream.id), "test-topic", 1, "none", 0, "never_expire", 0,
                         "server_default");

    rust::Vec<iggy::ffi::Message> messages;
    for (std::uint32_t i = 0; i < 5; i++) {
        iggy::ffi::Message msg;
        msg.new_message(to_payload("msg-" + std::to_string(i)));
        messages.push_back(std::move(msg));
    }

    client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), "partition_id",
                          partition_id_bytes(0), std::move(messages));

    auto polled = client->poll_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), 0, "consumer",
                                        make_numeric_identifier(1), "offset", 999999, 100, false);

    ASSERT_EQ(polled.count, 0u);
    ASSERT_EQ(polled.messages.size(), 0u);

    client->delete_stream(make_numeric_identifier(stream.id));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_Message, PollMessagesFirstStrategy) {
    RecordProperty("description", "Verifies first polling strategy returns messages from the beginning.");
    const std::string stream_name = "cpp-msg-first-strategy";
    iggy::ffi::Client *client     = login_to_server();
    ASSERT_NE(client, nullptr);

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    client->create_topic(make_numeric_identifier(stream.id), "test-topic", 1, "none", 0, "never_expire", 0,
                         "server_default");

    rust::Vec<iggy::ffi::Message> messages;
    for (std::uint32_t i = 0; i < 10; i++) {
        iggy::ffi::Message msg;
        msg.new_message(to_payload("msg-" + std::to_string(i)));
        messages.push_back(std::move(msg));
    }

    client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), "partition_id",
                          partition_id_bytes(0), std::move(messages));

    auto polled = client->poll_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), 0, "consumer",
                                        make_numeric_identifier(1), "first", 0, 3, false);

    ASSERT_EQ(polled.count, 3u);
    ASSERT_EQ(polled.messages.size(), 3u);
    EXPECT_EQ(polled.messages[0].offset, 0u);

    client->delete_stream(make_numeric_identifier(stream.id));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_Message, PollMessagesLastStrategy) {
    RecordProperty("description", "Verifies last polling strategy returns messages from the end.");
    const std::string stream_name = "cpp-msg-last-strategy";
    iggy::ffi::Client *client     = login_to_server();
    ASSERT_NE(client, nullptr);

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    client->create_topic(make_numeric_identifier(stream.id), "test-topic", 1, "none", 0, "never_expire", 0,
                         "server_default");

    rust::Vec<iggy::ffi::Message> messages;
    for (std::uint32_t i = 0; i < 10; i++) {
        iggy::ffi::Message msg;
        msg.new_message(to_payload("msg-" + std::to_string(i)));
        messages.push_back(std::move(msg));
    }

    client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), "partition_id",
                          partition_id_bytes(0), std::move(messages));

    auto polled = client->poll_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), 0, "consumer",
                                        make_numeric_identifier(1), "last", 0, 3, false);

    ASSERT_EQ(polled.count, 3u);
    ASSERT_EQ(polled.messages.size(), 3u);
    EXPECT_EQ(polled.messages[0].offset, 7u);
    EXPECT_EQ(polled.messages[2].offset, 9u);

    client->delete_stream(make_numeric_identifier(stream.id));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_Message, PollMessagesNextStrategyNoAutoCommit) {
    RecordProperty("description",
                   "Verifies next strategy without auto-commit returns the same messages on repeated calls.");
    const std::string stream_name = "cpp-msg-next-no-commit";
    iggy::ffi::Client *client     = login_to_server();
    ASSERT_NE(client, nullptr);

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    client->create_topic(make_numeric_identifier(stream.id), "test-topic", 1, "none", 0, "never_expire", 0,
                         "server_default");

    rust::Vec<iggy::ffi::Message> messages;
    for (std::uint32_t i = 0; i < 5; i++) {
        iggy::ffi::Message msg;
        msg.new_message(to_payload("msg-" + std::to_string(i)));
        messages.push_back(std::move(msg));
    }

    client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), "partition_id",
                          partition_id_bytes(0), std::move(messages));

    auto polled1 = client->poll_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), 0, "consumer",
                                         make_numeric_identifier(1), "next", 0, 100, false);
    ASSERT_EQ(polled1.count, 5u);

    auto polled2 = client->poll_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), 0, "consumer",
                                         make_numeric_identifier(1), "next", 0, 100, false);
    ASSERT_EQ(polled2.count, 5u);

    client->delete_stream(make_numeric_identifier(stream.id));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_Message, PollMessagesNextStrategyAutoCommit) {
    RecordProperty("description", "Verifies next strategy with auto-commit advances the offset on subsequent polls.");
    const std::string stream_name = "cpp-msg-next-auto-commit";
    iggy::ffi::Client *client     = login_to_server();
    ASSERT_NE(client, nullptr);

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    client->create_topic(make_numeric_identifier(stream.id), "test-topic", 1, "none", 0, "never_expire", 0,
                         "server_default");

    rust::Vec<iggy::ffi::Message> messages;
    for (std::uint32_t i = 0; i < 10; i++) {
        iggy::ffi::Message msg;
        msg.new_message(to_payload("msg-" + std::to_string(i)));
        messages.push_back(std::move(msg));
    }

    client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), "partition_id",
                          partition_id_bytes(0), std::move(messages));

    auto polled1 = client->poll_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), 0, "consumer",
                                         make_numeric_identifier(1), "next", 0, 5, true);
    ASSERT_EQ(polled1.count, 5u);
    EXPECT_EQ(polled1.messages[0].offset, 0u);
    EXPECT_EQ(polled1.messages[4].offset, 4u);

    auto polled2 = client->poll_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), 0, "consumer",
                                         make_numeric_identifier(1), "next", 0, 5, true);
    ASSERT_EQ(polled2.count, 5u);
    EXPECT_EQ(polled2.messages[0].offset, 5u);
    EXPECT_EQ(polled2.messages[4].offset, 9u);

    auto polled3 = client->poll_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), 0, "consumer",
                                         make_numeric_identifier(1), "next", 0, 5, true);
    ASSERT_EQ(polled3.count, 0u);

    client->delete_stream(make_numeric_identifier(stream.id));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_Message, PollMessagesConsumerIdIndependence) {
    RecordProperty("description", "Verifies different consumer IDs maintain independent offsets.");
    const std::string stream_name = "cpp-msg-consumer-indep";
    iggy::ffi::Client *client     = login_to_server();
    ASSERT_NE(client, nullptr);

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    client->create_topic(make_numeric_identifier(stream.id), "test-topic", 1, "none", 0, "never_expire", 0,
                         "server_default");

    rust::Vec<iggy::ffi::Message> messages;
    for (std::uint32_t i = 0; i < 5; i++) {
        iggy::ffi::Message msg;
        msg.new_message(to_payload("msg-" + std::to_string(i)));
        messages.push_back(std::move(msg));
    }

    client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), "partition_id",
                          partition_id_bytes(0), std::move(messages));

    auto polled_c1 = client->poll_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), 0,
                                           "consumer", make_numeric_identifier(1), "next", 0, 3, true);
    ASSERT_EQ(polled_c1.count, 3u);

    auto polled_c2 = client->poll_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), 0,
                                           "consumer", make_numeric_identifier(2), "next", 0, 5, true);
    ASSERT_EQ(polled_c2.count, 5u);

    auto polled_c1_again = client->poll_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), 0,
                                                 "consumer", make_numeric_identifier(1), "next", 0, 5, true);
    ASSERT_EQ(polled_c1_again.count, 2u);

    client->delete_stream(make_numeric_identifier(stream.id));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_Message, PollMessagesMultipleSendsThenPollOrder) {
    RecordProperty("description", "Verifies message ordering is preserved across multiple send batches.");
    const std::string stream_name = "cpp-msg-multi-batch-order";
    iggy::ffi::Client *client     = login_to_server();
    ASSERT_NE(client, nullptr);

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    client->create_topic(make_numeric_identifier(stream.id), "test-topic", 1, "none", 0, "never_expire", 0,
                         "server_default");

    rust::Vec<iggy::ffi::Message> batch1;
    for (std::uint32_t i = 0; i < 5; i++) {
        iggy::ffi::Message msg;
        msg.new_message(to_payload("batch1-" + std::to_string(i)));
        batch1.push_back(std::move(msg));
    }
    client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), "partition_id",
                          partition_id_bytes(0), std::move(batch1));

    rust::Vec<iggy::ffi::Message> batch2;
    for (std::uint32_t i = 0; i < 5; i++) {
        iggy::ffi::Message msg;
        msg.new_message(to_payload("batch2-" + std::to_string(i)));
        batch2.push_back(std::move(msg));
    }
    client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), "partition_id",
                          partition_id_bytes(0), std::move(batch2));

    auto polled = client->poll_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), 0, "consumer",
                                        make_numeric_identifier(1), "offset", 0, 100, false);

    ASSERT_EQ(polled.count, 10u);
    for (std::uint32_t i = 0; i < 5; i++) {
        std::string expected = "batch1-" + std::to_string(i);
        std::string actual(polled.messages[i].payload.begin(), polled.messages[i].payload.end());
        EXPECT_EQ(actual, expected) << "batch1 payload mismatch at index " << i;
    }
    for (std::uint32_t i = 0; i < 5; i++) {
        std::string expected = "batch2-" + std::to_string(i);
        std::string actual(polled.messages[5 + i].payload.begin(), polled.messages[5 + i].payload.end());
        EXPECT_EQ(actual, expected) << "batch2 payload mismatch at index " << i;
    }

    client->delete_stream(make_numeric_identifier(stream.id));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_Message, PollMessagesMultipleCustomIds) {
    RecordProperty("description", "Verifies multiple messages with distinct custom IDs are all preserved.");
    const std::string stream_name = "cpp-msg-multi-custom-ids";
    iggy::ffi::Client *client     = login_to_server();
    ASSERT_NE(client, nullptr);

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    client->create_topic(make_numeric_identifier(stream.id), "test-topic", 1, "none", 0, "never_expire", 0,
                         "server_default");

    const std::uint64_t id_values[] = {100, 200, 300, 400, 500};
    rust::Vec<iggy::ffi::Message> messages;
    for (std::uint32_t i = 0; i < 5; i++) {
        iggy::ffi::Message msg;
        msg.new_message(to_payload("msg-" + std::to_string(i)));
        msg.id_lo = id_values[i];
        msg.id_hi = 0;
        messages.push_back(std::move(msg));
    }

    client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), "partition_id",
                          partition_id_bytes(0), std::move(messages));

    auto polled = client->poll_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), 0, "consumer",
                                        make_numeric_identifier(1), "offset", 0, 100, false);

    ASSERT_EQ(polled.count, 5u);
    for (std::uint32_t i = 0; i < 5; i++) {
        EXPECT_EQ(polled.messages[i].id_lo, id_values[i]) << "ID mismatch at index " << i;
        EXPECT_EQ(polled.messages[i].id_hi, 0u);
    }

    client->delete_stream(make_numeric_identifier(stream.id));
    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}

TEST(LowLevelE2E_Message, PollMessagesAfterStreamDeleted) {
    RecordProperty("description", "Throws when polling messages after the stream has been deleted.");
    const std::string stream_name = "cpp-msg-deleted-stream";
    iggy::ffi::Client *client     = login_to_server();
    ASSERT_NE(client, nullptr);

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    client->create_topic(make_numeric_identifier(stream.id), "test-topic", 1, "none", 0, "never_expire", 0,
                         "server_default");

    rust::Vec<iggy::ffi::Message> messages;
    iggy::ffi::Message msg;
    msg.new_message(to_payload("test"));
    messages.push_back(std::move(msg));

    client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), "partition_id",
                          partition_id_bytes(0), std::move(messages));

    std::uint32_t saved_stream_id = stream.id;
    client->delete_stream(make_numeric_identifier(saved_stream_id));

    ASSERT_THROW(client->poll_messages(make_numeric_identifier(saved_stream_id), make_numeric_identifier(0), 0,
                                       "consumer", make_numeric_identifier(1), "offset", 0, 10, false),
                 std::exception);

    ASSERT_NO_THROW(iggy::ffi::delete_connection(client));
}
