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
#include <thread>

#include <gtest/gtest.h>

#include "lib.rs.h"
#include "tests/common/test_helpers.hpp"

class LowLevelE2E_Message : public E2ETestFixture {};

TEST_F(LowLevelE2E_Message, SendAndPollMessagesRoundTrip) {
    RecordProperty("description", "Sends 10 messages and polls them back, verifying count, offsets, and payloads.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    TrackStream(stream.id);
    const std::string topic_name = GetRandomName();
    client->create_topic(make_numeric_identifier(stream.id), topic_name, 1, "none", 0, "never_expire", 0,
                         "server_default");

    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    for (std::uint32_t i = 0; i < 10; i++) {
        auto msg = iggy::ffi::make_message(to_payload("test message " + std::to_string(i)),
                                           rust::Vec<iggy::ffi::HeaderEntry>());
        messages.push_back(std::move(msg));
    }

    ASSERT_NO_THROW(client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0),
                                          "partition_id", partition_id_bytes(0), std::move(messages)));

    auto polled = client->poll_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), 0, "consumer",
                                        make_numeric_identifier(1), "offset", 0, 100, false);

    ASSERT_EQ(polled.partition_id, 0u) << "Polled partition_id mismatches the partition we sent to";
    ASSERT_EQ(polled.count, 10u);
    ASSERT_EQ(polled.messages.size(), 10u);
    for (std::uint32_t i = 0; i < 10; i++) {
        ASSERT_EQ(polled.messages[i].offset, static_cast<std::uint64_t>(i));
        std::string expected = "test message " + std::to_string(i);
        std::string actual(polled.messages[i].payload.begin(), polled.messages[i].payload.end());
        ASSERT_EQ(actual, expected) << "Payload mismatch at offset " << i;
    }
}

TEST_F(LowLevelE2E_Message, PollMessagesVerifyMessageIds) {
    RecordProperty("description", "Verifies that polled message IDs match the sent IDs.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    TrackStream(stream.id);
    const std::string topic_name = GetRandomName();
    client->create_topic(make_numeric_identifier(stream.id), topic_name, 1, "none", 0, "never_expire", 0,
                         "server_default");

    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    auto msg  = iggy::ffi::make_message(to_payload("id-test-message"), rust::Vec<iggy::ffi::HeaderEntry>());
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
}

TEST_F(LowLevelE2E_Message, PollMessagesFromEmptyPartition) {
    RecordProperty("description", "Verifies polling from an empty partition returns zero messages.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    TrackStream(stream.id);
    const std::string topic_name = GetRandomName();
    client->create_topic(make_numeric_identifier(stream.id), topic_name, 1, "none", 0, "never_expire", 0,
                         "server_default");

    auto polled = client->poll_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), 0, "consumer",
                                        make_numeric_identifier(1), "offset", 0, 100, false);

    ASSERT_EQ(polled.count, 0u);
    ASSERT_EQ(polled.messages.size(), 0u);
}

TEST_F(LowLevelE2E_Message, SendMessagesBeforeLoginThrows) {
    RecordProperty("description", "Verifies send_messages throws when not authenticated.");
    iggy::ffi::Client *client = GetLoggedOutClient();
    ASSERT_NO_THROW(client->connect());

    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    auto msg = iggy::ffi::make_message(to_payload("should-fail"), rust::Vec<iggy::ffi::HeaderEntry>());
    messages.push_back(std::move(msg));

    ASSERT_THROW(client->send_messages(make_numeric_identifier(1), make_numeric_identifier(1), "partition_id",
                                       partition_id_bytes(0), std::move(messages)),
                 std::exception);
    ASSERT_NO_THROW(client->login_user("iggy", "iggy"));
    ASSERT_NO_THROW(client->disconnect());

    rust::Vec<iggy::ffi::IggyMessageToSend> disconnected_messages;
    auto disconnected_msg =
        iggy::ffi::make_message(to_payload("should-still-fail"), rust::Vec<iggy::ffi::HeaderEntry>());
    disconnected_messages.push_back(std::move(disconnected_msg));
    ASSERT_THROW(client->send_messages(make_numeric_identifier(1), make_numeric_identifier(1), "partition_id",
                                       partition_id_bytes(0), std::move(disconnected_messages)),
                 std::exception);
}

TEST_F(LowLevelE2E_Message, SendMessagesWithInvalidStreamId) {
    RecordProperty("description", "Throws when sending messages with an invalid stream identifier.");
    iggy::ffi::Client *client = GetLoggedInClient();

    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    auto msg = iggy::ffi::make_message(to_payload("test"), rust::Vec<iggy::ffi::HeaderEntry>());
    messages.push_back(std::move(msg));

    iggy::ffi::Identifier invalid_id;
    invalid_id.kind   = "invalid";
    invalid_id.length = 0;

    ASSERT_THROW(client->send_messages(invalid_id, make_numeric_identifier(1), "partition_id", partition_id_bytes(0),
                                       std::move(messages)),
                 std::exception);
}

TEST_F(LowLevelE2E_Message, SendMessagesToNonExistentStream) {
    RecordProperty("description", "Throws when sending messages to a non-existent stream.");
    iggy::ffi::Client *client = GetLoggedInClient();

    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    auto msg = iggy::ffi::make_message(to_payload("test"), rust::Vec<iggy::ffi::HeaderEntry>());
    messages.push_back(std::move(msg));

    ASSERT_THROW(client->send_messages(make_string_identifier("nonexistent-stream-12345"), make_numeric_identifier(0),
                                       "partition_id", partition_id_bytes(0), std::move(messages)),
                 std::exception);
}

TEST_F(LowLevelE2E_Message, SendMessagesWithInvalidPartitioningKind) {
    RecordProperty("description", "Throws when sending messages with an invalid partitioning kind.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    TrackStream(stream.id);
    const std::string topic_name = GetRandomName();
    client->create_topic(make_numeric_identifier(stream.id), topic_name, 1, "none", 0, "never_expire", 0,
                         "server_default");

    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    auto msg = iggy::ffi::make_message(to_payload("test"), rust::Vec<iggy::ffi::HeaderEntry>());
    messages.push_back(std::move(msg));

    ASSERT_THROW(client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), "invalid_kind",
                                       partition_id_bytes(0), std::move(messages)),
                 std::exception);
}

TEST_F(LowLevelE2E_Message, SendMessagesWithInvalidPartitioningValue) {
    RecordProperty("description", "Throws when sending messages with insufficient partitioning value bytes.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    TrackStream(stream.id);
    const std::string topic_name = GetRandomName();
    client->create_topic(make_numeric_identifier(stream.id), topic_name, 1, "none", 0, "never_expire", 0,
                         "server_default");

    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    auto msg = iggy::ffi::make_message(to_payload("test"), rust::Vec<iggy::ffi::HeaderEntry>());
    messages.push_back(std::move(msg));

    rust::Vec<std::uint8_t> short_bytes;
    short_bytes.push_back(0x00);
    short_bytes.push_back(0x01);

    ASSERT_THROW(client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), "partition_id",
                                       std::move(short_bytes), std::move(messages)),
                 std::exception);
}

TEST_F(LowLevelE2E_Message, SendMessagesToSpecificPartitionVerified) {
    RecordProperty("description",
                   "Verifies messages sent to a specific partition are only retrievable from that partition.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    TrackStream(stream.id);
    const std::string topic_name = GetRandomName();
    client->create_topic(make_numeric_identifier(stream.id), topic_name, 3, "none", 0, "never_expire", 0,
                         "server_default");

    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    for (std::uint32_t i = 0; i < 5; i++) {
        auto msg = iggy::ffi::make_message(to_payload("partition-test-" + std::to_string(i)),
                                           rust::Vec<iggy::ffi::HeaderEntry>());
        messages.push_back(std::move(msg));
    }

    client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), "partition_id",
                          partition_id_bytes(0), std::move(messages));

    auto polled_part0 = client->poll_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), 0,
                                              "consumer", make_numeric_identifier(1), "offset", 0, 100, false);
    ASSERT_EQ(polled_part0.partition_id, 0u);
    ASSERT_EQ(polled_part0.count, 5u);

    auto polled_part1 = client->poll_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), 1,
                                              "consumer", make_numeric_identifier(1), "offset", 0, 100, false);
    ASSERT_EQ(polled_part1.partition_id, 1u);
    ASSERT_EQ(polled_part1.count, 0u);
}

TEST_F(LowLevelE2E_Message, SendEmptyMessageVectorThrows) {
    RecordProperty("description", "Throws when sending an empty message vector.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    TrackStream(stream.id);
    const std::string topic_name = GetRandomName();
    client->create_topic(make_numeric_identifier(stream.id), topic_name, 1, "none", 0, "never_expire", 0,
                         "server_default");

    rust::Vec<iggy::ffi::IggyMessageToSend> empty_messages;

    ASSERT_THROW(client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), "partition_id",
                                       partition_id_bytes(0), std::move(empty_messages)),
                 std::exception);
}

TEST_F(LowLevelE2E_Message, SendMessageWithEmptyPayloadThrows) {
    RecordProperty("description", "Throws when sending a message with an empty payload.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    TrackStream(stream.id);
    const std::string topic_name = GetRandomName();
    client->create_topic(make_numeric_identifier(stream.id), topic_name, 1, "none", 0, "never_expire", 0,
                         "server_default");

    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    rust::Vec<std::uint8_t> empty_payload;
    auto msg = iggy::ffi::make_message(std::move(empty_payload), rust::Vec<iggy::ffi::HeaderEntry>());
    messages.push_back(std::move(msg));

    ASSERT_THROW(client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), "partition_id",
                                       partition_id_bytes(0), std::move(messages)),
                 std::exception);
}

TEST_F(LowLevelE2E_Message, SendMessageWithOversizedPayloadThrows) {
    RecordProperty("description", "Throws when sending a message exceeding maximum payload size.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    TrackStream(stream.id);
    const std::string topic_name = GetRandomName();
    client->create_topic(make_numeric_identifier(stream.id), topic_name, 1, "none", 0, "never_expire", 0,
                         "server_default");

    // Build a payload one byte over the SDK's max payload size (64 MB).
    constexpr std::uint32_t kOversizedPayloadBytes = 64'000'001u;
    rust::Vec<std::uint8_t> oversized_payload;
    oversized_payload.reserve(kOversizedPayloadBytes);
    for (std::uint32_t i = 0; i < kOversizedPayloadBytes; i++) {
        oversized_payload.push_back(0x41);
    }

    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    auto msg = iggy::ffi::make_message(std::move(oversized_payload), rust::Vec<iggy::ffi::HeaderEntry>());
    messages.push_back(std::move(msg));

    ASSERT_THROW(client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), "partition_id",
                                       partition_id_bytes(0), std::move(messages)),
                 std::exception);
}

TEST_F(LowLevelE2E_Message, SendMessagesPreservesOrder) {
    RecordProperty("description", "Verifies messages are stored and retrieved in the order they were sent.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    TrackStream(stream.id);
    const std::string topic_name = GetRandomName();
    client->create_topic(make_numeric_identifier(stream.id), topic_name, 1, "none", 0, "never_expire", 0,
                         "server_default");

    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    for (std::uint32_t i = 0; i < 50; i++) {
        auto msg =
            iggy::ffi::make_message(to_payload("order-" + std::to_string(i)), rust::Vec<iggy::ffi::HeaderEntry>());
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
}

TEST_F(LowLevelE2E_Message, SendMessagesWithDuplicateIds) {
    RecordProperty("description", "Verifies sending multiple messages with the same ID succeeds.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    TrackStream(stream.id);
    const std::string topic_name = GetRandomName();
    client->create_topic(make_numeric_identifier(stream.id), topic_name, 1, "none", 0, "never_expire", 0,
                         "server_default");

    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    for (std::uint32_t i = 0; i < 3; i++) {
        auto msg =
            iggy::ffi::make_message(to_payload("dup-id-msg-" + std::to_string(i)), rust::Vec<iggy::ffi::HeaderEntry>());
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
}

TEST_F(LowLevelE2E_Message, SendMessagesWithVariousPayloads) {
    RecordProperty("description",
                   "Verifies various payload types including null bytes, UTF-8, and binary data are preserved.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    TrackStream(stream.id);
    const std::string topic_name = GetRandomName();
    client->create_topic(make_numeric_identifier(stream.id), topic_name, 1, "none", 0, "never_expire", 0,
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

    rust::Vec<iggy::ffi::IggyMessageToSend> messages;

    auto msg0 = iggy::ffi::make_message(to_payload("simple ascii"), rust::Vec<iggy::ffi::HeaderEntry>());
    messages.push_back(std::move(msg0));

    auto msg1 = iggy::ffi::make_message(std::move(payload_null), rust::Vec<iggy::ffi::HeaderEntry>());
    messages.push_back(std::move(msg1));

    auto msg2 = iggy::ffi::make_message(to_payload("héllo wörld"), rust::Vec<iggy::ffi::HeaderEntry>());
    messages.push_back(std::move(msg2));

    auto msg3 = iggy::ffi::make_message(std::move(payload_binary), rust::Vec<iggy::ffi::HeaderEntry>());
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
}

TEST_F(LowLevelE2E_Message, SendAndPollMessageWithTypedHeadersRoundTrip) {
    RecordProperty(
        "description",
        "Sends one message per typed header kind and verifies payload, IDs, header contents, and encoded size.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    TrackStream(stream.id);
    const std::string topic_name = GetRandomName();
    client->create_topic(make_numeric_identifier(stream.id), topic_name, 1, "none", 0, "never_expire", 0,
                         "server_default");

    struct ExpectedHeaderMessage {
        const char *key;
        iggy::ffi::HeaderKind value_kind;
        const rust::Vec<std::uint8_t> *value;
    };

    const rust::Vec<std::uint8_t> raw_value{0xDE, 0xAD, 0xBE, 0xEF};                            // raw bytes 0xDEADBEEF
    const rust::Vec<std::uint8_t> string_value = to_payload("hello");                           // UTF-8 string "hello"
    const rust::Vec<std::uint8_t> bool_value{0x01};                                             // bool true
    const rust::Vec<std::uint8_t> int8_value{0xFB};                                             // int8 -5
    const rust::Vec<std::uint8_t> int16_value{0x2E, 0xFB};                                      // int16 -1234
    const rust::Vec<std::uint8_t> int32_value{0xEB, 0x32, 0xA4, 0xF8};                          // int32 -123456789
    const rust::Vec<std::uint8_t> int64_value{0x79, 0x29, 0xED, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF};  // int64 -1234567
    const rust::Vec<std::uint8_t> int128_value{
        0x00, 0xFF, 0xEE, 0xDD, 0xCC, 0xBB, 0xAA, 0x99,
        0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11};                 // int128 0x112233445566778899AABBCCDDEEFF00
    const rust::Vec<std::uint8_t> uint8_value{0xFA};                     // uint8 250
    const rust::Vec<std::uint8_t> uint16_value{0xD2, 0x04};              // uint16 1234
    const rust::Vec<std::uint8_t> uint32_value{0x78, 0x56, 0x34, 0x12};  // uint32 0x12345678
    const rust::Vec<std::uint8_t> uint64_value{0x88, 0x77, 0x66, 0x55,
                                               0x44, 0x33, 0x22, 0x11};  // uint64 0x1122334455667788
    const rust::Vec<std::uint8_t> uint128_value{
        0x10, 0x32, 0x54, 0x76, 0x98, 0xBA, 0xDC, 0xFE,
        0xEF, 0xCD, 0xAB, 0x89, 0x67, 0x45, 0x23, 0x01};                  // uint128 0x0123456789ABCDEFFEDCBA9876543210
    const rust::Vec<std::uint8_t> float32_value{0x00, 0x00, 0x80, 0x3F};  // float32 1.0
    const rust::Vec<std::uint8_t> float64_value{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F};  // float64 1.0
    const ExpectedHeaderMessage expected_messages[] = {
        {"raw", iggy::ffi::HeaderKind::Raw, &raw_value},
        {"string", iggy::ffi::HeaderKind::String, &string_value},
        {"bool", iggy::ffi::HeaderKind::Bool, &bool_value},
        {"int8", iggy::ffi::HeaderKind::Int8, &int8_value},
        {"int16", iggy::ffi::HeaderKind::Int16, &int16_value},
        {"int32", iggy::ffi::HeaderKind::Int32, &int32_value},
        {"int64", iggy::ffi::HeaderKind::Int64, &int64_value},
        {"int128", iggy::ffi::HeaderKind::Int128, &int128_value},
        {"uint8", iggy::ffi::HeaderKind::UInt8, &uint8_value},
        {"uint16", iggy::ffi::HeaderKind::UInt16, &uint16_value},
        {"uint32", iggy::ffi::HeaderKind::UInt32, &uint32_value},
        {"uint64", iggy::ffi::HeaderKind::UInt64, &uint64_value},
        {"uint128", iggy::ffi::HeaderKind::UInt128, &uint128_value},
        {"float32", iggy::ffi::HeaderKind::Float32, &float32_value},
        {"float64", iggy::ffi::HeaderKind::Float64, &float64_value},
    };
    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    for (const auto &expected : expected_messages) {
        rust::Vec<iggy::ffi::HeaderEntry> headers;
        headers.push_back(make_header_entry(make_header_field(iggy::ffi::HeaderKind::String, to_payload(expected.key)),
                                            make_header_field(expected.value_kind, *expected.value)));

        messages.push_back(
            iggy::ffi::make_message(to_payload(std::string("payload-") + expected.key), std::move(headers)));
    }

    ASSERT_NO_THROW(client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0),
                                          "partition_id", partition_id_bytes(0), std::move(messages)));

    auto polled = client->poll_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), 0, "consumer",
                                        make_numeric_identifier(1), "offset", 0, 100, false);

    constexpr std::size_t expected_message_count = sizeof(expected_messages) / sizeof(expected_messages[0]);
    ASSERT_EQ(polled.count, expected_message_count);
    ASSERT_EQ(polled.messages.size(), expected_message_count);
    for (std::size_t i = 0; i < expected_message_count; ++i) {
        const auto &expected       = expected_messages[i];
        const auto &polled_message = polled.messages[i];

        EXPECT_EQ(std::string(polled_message.payload.begin(), polled_message.payload.end()),
                  std::string("payload-") + expected.key);
        ASSERT_EQ(polled_message.user_headers.size(), 1u);
        EXPECT_EQ(polled_message.user_headers_length,
                  static_cast<std::uint32_t>(10 + std::string(expected.key).size() + expected.value->size()));
        EXPECT_TRUE(has_header(polled_message.user_headers, static_cast<std::uint8_t>(iggy::ffi::HeaderKind::String),
                               to_payload(expected.key), static_cast<std::uint8_t>(expected.value_kind),
                               *expected.value));
    }
}

TEST_F(LowLevelE2E_Message, SendMessageWithDuplicateTypedHeaderKeysThrows) {
    RecordProperty("description", "Throws when a single message contains duplicate typed header keys.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    TrackStream(stream.id);
    const std::string topic_name = GetRandomName();
    client->create_topic(make_numeric_identifier(stream.id), topic_name, 1, "none", 0, "never_expire", 0,
                         "server_default");

    rust::Vec<iggy::ffi::HeaderEntry> headers;
    headers.push_back(make_header_entry(make_header_field(iggy::ffi::HeaderKind::String, to_payload("dup-key")),
                                        make_header_field(iggy::ffi::HeaderKind::String, to_payload("first"))));
    headers.push_back(make_header_entry(make_header_field(iggy::ffi::HeaderKind::String, to_payload("dup-key")),
                                        make_header_field(iggy::ffi::HeaderKind::String, to_payload("second"))));

    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    messages.push_back(iggy::ffi::make_message(to_payload("payload"), std::move(headers)));

    ASSERT_THROW(client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), "partition_id",
                                       partition_id_bytes(0), std::move(messages)),
                 std::exception);
}

TEST_F(LowLevelE2E_Message, SendMessageWithWrongFixedWidthHeaderBytesThrows) {
    RecordProperty("description", "Throws when a typed header uses a fixed-width kind with the wrong byte count.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    TrackStream(stream.id);
    const std::string topic_name = GetRandomName();
    client->create_topic(make_numeric_identifier(stream.id), topic_name, 1, "none", 0, "never_expire", 0,
                         "server_default");

    struct FixedWidthHeaderCase {
        const char *key;
        iggy::ffi::HeaderKind kind;
    };
    const FixedWidthHeaderCase fixed_width_header_cases[] = {
        {"broken-bool", iggy::ffi::HeaderKind::Bool},       {"broken-int8", iggy::ffi::HeaderKind::Int8},
        {"broken-int16", iggy::ffi::HeaderKind::Int16},     {"broken-int32", iggy::ffi::HeaderKind::Int32},
        {"broken-int64", iggy::ffi::HeaderKind::Int64},     {"broken-int128", iggy::ffi::HeaderKind::Int128},
        {"broken-uint8", iggy::ffi::HeaderKind::UInt8},     {"broken-uint16", iggy::ffi::HeaderKind::UInt16},
        {"broken-uint32", iggy::ffi::HeaderKind::UInt32},   {"broken-uint64", iggy::ffi::HeaderKind::UInt64},
        {"broken-uint128", iggy::ffi::HeaderKind::UInt128}, {"broken-float32", iggy::ffi::HeaderKind::Float32},
        {"broken-float64", iggy::ffi::HeaderKind::Float64},
    };

    for (const auto &test_case : fixed_width_header_cases) {
        SCOPED_TRACE(test_case.key);

        rust::Vec<iggy::ffi::HeaderEntry> headers;
        rust::Vec<std::uint8_t> broken_width_value;
        broken_width_value.push_back(0x01);
        broken_width_value.push_back(0x02);
        broken_width_value.push_back(0x03);
        headers.push_back(make_header_entry(make_header_field(iggy::ffi::HeaderKind::String, to_payload(test_case.key)),
                                            make_header_field(test_case.kind, std::move(broken_width_value))));

        rust::Vec<iggy::ffi::IggyMessageToSend> messages;
        messages.push_back(iggy::ffi::make_message(to_payload("payload"), std::move(headers)));

        ASSERT_THROW(client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0),
                                           "partition_id", partition_id_bytes(0), std::move(messages)),
                     std::exception);
    }
}

TEST_F(LowLevelE2E_Message, SendMessageWithInvalidTypedHeaderKindThrows) {
    RecordProperty("description", "Throws when a typed header uses an unsupported header kind code.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    TrackStream(stream.id);
    const std::string topic_name = GetRandomName();
    client->create_topic(make_numeric_identifier(stream.id), topic_name, 1, "none", 0, "never_expire", 0,
                         "server_default");

    iggy::ffi::HeaderField invalid_key;
    invalid_key.kind  = 255;
    invalid_key.value = to_payload("bad-kind");

    rust::Vec<iggy::ffi::HeaderEntry> headers;
    headers.push_back(make_header_entry(std::move(invalid_key),
                                        make_header_field(iggy::ffi::HeaderKind::String, to_payload("value"))));

    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    messages.push_back(iggy::ffi::make_message(to_payload("payload"), std::move(headers)));

    ASSERT_THROW(client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), "partition_id",
                                       partition_id_bytes(0), std::move(messages)),
                 std::exception);
}

TEST_F(LowLevelE2E_Message, SendMessageWithInvalidTypedHeaderSizesThrows) {
    RecordProperty("description", "Throws when typed header key or value sizes violate Rust header size constraints.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    TrackStream(stream.id);
    const std::string topic_name = GetRandomName();
    client->create_topic(make_numeric_identifier(stream.id), topic_name, 1, "none", 0, "never_expire", 0,
                         "server_default");

    rust::Vec<iggy::ffi::HeaderEntry> empty_key_headers;
    empty_key_headers.push_back(
        make_header_entry(make_header_field(iggy::ffi::HeaderKind::String, rust::Vec<std::uint8_t>()),
                          make_header_field(iggy::ffi::HeaderKind::String, to_payload("value"))));
    rust::Vec<iggy::ffi::IggyMessageToSend> empty_key_messages;
    empty_key_messages.push_back(iggy::ffi::make_message(to_payload("payload"), std::move(empty_key_headers)));
    ASSERT_THROW(client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), "partition_id",
                                       partition_id_bytes(0), std::move(empty_key_messages)),
                 std::exception);

    rust::Vec<iggy::ffi::HeaderEntry> empty_raw_value_headers;
    empty_raw_value_headers.push_back(
        make_header_entry(make_header_field(iggy::ffi::HeaderKind::String, to_payload("key")),
                          make_header_field(iggy::ffi::HeaderKind::Raw, rust::Vec<std::uint8_t>())));
    rust::Vec<iggy::ffi::IggyMessageToSend> empty_raw_value_messages;
    empty_raw_value_messages.push_back(
        iggy::ffi::make_message(to_payload("payload"), std::move(empty_raw_value_headers)));
    ASSERT_THROW(client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), "partition_id",
                                       partition_id_bytes(0), std::move(empty_raw_value_messages)),
                 std::exception);

    rust::Vec<std::uint8_t> oversized_value_bytes;
    for (std::size_t index = 0; index < 256; ++index) {
        oversized_value_bytes.push_back(static_cast<std::uint8_t>(index));
    }
    rust::Vec<iggy::ffi::HeaderEntry> oversized_value_headers;
    oversized_value_headers.push_back(
        make_header_entry(make_header_field(iggy::ffi::HeaderKind::String, to_payload("key")),
                          make_header_field(iggy::ffi::HeaderKind::Raw, std::move(oversized_value_bytes))));
    rust::Vec<iggy::ffi::IggyMessageToSend> oversized_value_messages;
    oversized_value_messages.push_back(
        iggy::ffi::make_message(to_payload("payload"), std::move(oversized_value_headers)));
    ASSERT_THROW(client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), "partition_id",
                                       partition_id_bytes(0), std::move(oversized_value_messages)),
                 std::exception);

    rust::Vec<std::uint8_t> oversized_string_bytes;
    for (std::size_t index = 0; index < 256; ++index) {
        oversized_string_bytes.push_back('a');
    }
    rust::Vec<iggy::ffi::HeaderEntry> oversized_string_headers;
    oversized_string_headers.push_back(
        make_header_entry(make_header_field(iggy::ffi::HeaderKind::String, to_payload("key")),
                          make_header_field(iggy::ffi::HeaderKind::String, std::move(oversized_string_bytes))));
    rust::Vec<iggy::ffi::IggyMessageToSend> oversized_string_messages;
    oversized_string_messages.push_back(
        iggy::ffi::make_message(to_payload("payload"), std::move(oversized_string_headers)));
    ASSERT_THROW(client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), "partition_id",
                                       partition_id_bytes(0), std::move(oversized_string_messages)),
                 std::exception);
}

TEST_F(LowLevelE2E_Message, SendMessageAtUserHeadersSizeBoundary) {
    RecordProperty("description",
                   "Throws when encoded user headers exceed 100_000 bytes and succeeds at exactly 100_000 bytes.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    TrackStream(stream.id);
    const std::string topic_name = GetRandomName();
    client->create_topic(make_numeric_identifier(stream.id), topic_name, 1, "none", 0, "never_expire", 0,
                         "server_default");

    constexpr std::uint32_t kMaxUserHeadersBytes     = 100'000u;
    constexpr std::uint32_t kFullHeaderEncodedBytes  = 267u;  // 10 bytes framing + 2-byte key + 255-byte value
    constexpr std::uint32_t kFullHeaderCount         = 374u;
    constexpr std::uint32_t kTailExactValueBytes     = 130u;
    constexpr std::uint32_t kTailOversizedValueBytes = 131u;

    rust::Vec<iggy::ffi::HeaderEntry> oversized_headers;
    for (std::uint32_t index = 0; index < kFullHeaderCount; ++index) {
        rust::Vec<std::uint8_t> key_bytes;
        key_bytes.push_back(static_cast<std::uint8_t>(index & 0xFF));
        key_bytes.push_back(static_cast<std::uint8_t>((index >> 8) & 0xFF));

        rust::Vec<std::uint8_t> value_bytes;
        for (std::uint32_t value_index = 0; value_index < 255u; ++value_index) {
            value_bytes.push_back(static_cast<std::uint8_t>(value_index));
        }

        oversized_headers.push_back(
            make_header_entry(make_header_field(iggy::ffi::HeaderKind::Raw, std::move(key_bytes)),
                              make_header_field(iggy::ffi::HeaderKind::Raw, std::move(value_bytes))));
    }
    rust::Vec<std::uint8_t> oversized_tail_key_bytes;
    oversized_tail_key_bytes.push_back(static_cast<std::uint8_t>(kFullHeaderCount & 0xFF));
    oversized_tail_key_bytes.push_back(static_cast<std::uint8_t>((kFullHeaderCount >> 8) & 0xFF));
    rust::Vec<std::uint8_t> oversized_tail_value_bytes;
    for (std::uint32_t value_index = 0; value_index < kTailOversizedValueBytes; ++value_index) {
        oversized_tail_value_bytes.push_back(static_cast<std::uint8_t>(value_index));
    }
    oversized_headers.push_back(
        make_header_entry(make_header_field(iggy::ffi::HeaderKind::Raw, std::move(oversized_tail_key_bytes)),
                          make_header_field(iggy::ffi::HeaderKind::Raw, std::move(oversized_tail_value_bytes))));

    rust::Vec<iggy::ffi::IggyMessageToSend> oversized_messages;
    oversized_messages.push_back(
        iggy::ffi::make_message(to_payload("oversized-user-headers"), std::move(oversized_headers)));
    ASSERT_THROW(client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), "partition_id",
                                       partition_id_bytes(0), std::move(oversized_messages)),
                 std::exception);

    rust::Vec<iggy::ffi::HeaderEntry> exact_headers;
    for (std::uint32_t index = 0; index < kFullHeaderCount; ++index) {
        rust::Vec<std::uint8_t> key_bytes;
        key_bytes.push_back(static_cast<std::uint8_t>(index & 0xFF));
        key_bytes.push_back(static_cast<std::uint8_t>((index >> 8) & 0xFF));

        rust::Vec<std::uint8_t> value_bytes;
        for (std::uint32_t value_index = 0; value_index < 255u; ++value_index) {
            value_bytes.push_back(static_cast<std::uint8_t>(value_index));
        }

        exact_headers.push_back(
            make_header_entry(make_header_field(iggy::ffi::HeaderKind::Raw, std::move(key_bytes)),
                              make_header_field(iggy::ffi::HeaderKind::Raw, std::move(value_bytes))));
    }
    rust::Vec<std::uint8_t> exact_tail_key_bytes;
    exact_tail_key_bytes.push_back(static_cast<std::uint8_t>(kFullHeaderCount & 0xFF));
    exact_tail_key_bytes.push_back(static_cast<std::uint8_t>((kFullHeaderCount >> 8) & 0xFF));
    rust::Vec<std::uint8_t> exact_tail_value_bytes;
    for (std::uint32_t value_index = 0; value_index < kTailExactValueBytes; ++value_index) {
        exact_tail_value_bytes.push_back(static_cast<std::uint8_t>(value_index));
    }
    exact_headers.push_back(
        make_header_entry(make_header_field(iggy::ffi::HeaderKind::Raw, std::move(exact_tail_key_bytes)),
                          make_header_field(iggy::ffi::HeaderKind::Raw, std::move(exact_tail_value_bytes))));

    rust::Vec<iggy::ffi::IggyMessageToSend> exact_messages;
    exact_messages.push_back(iggy::ffi::make_message(to_payload("exact-user-headers"), std::move(exact_headers)));
    ASSERT_NO_THROW(client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0),
                                          "partition_id", partition_id_bytes(0), std::move(exact_messages)));

    auto polled = client->poll_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), 0, "consumer",
                                        make_numeric_identifier(1), "offset", 0, 10, false);

    ASSERT_EQ(polled.count, 1u);
    ASSERT_EQ(polled.messages.size(), 1u);
    EXPECT_EQ(std::string(polled.messages[0].payload.begin(), polled.messages[0].payload.end()), "exact-user-headers");
    EXPECT_EQ(polled.messages[0].user_headers_length, kMaxUserHeadersBytes);
    EXPECT_EQ(polled.messages[0].user_headers.size(), kFullHeaderCount + 1u);
    EXPECT_EQ(kFullHeaderCount * kFullHeaderEncodedBytes + 10u + 2u + kTailExactValueBytes, kMaxUserHeadersBytes);
}

TEST_F(LowLevelE2E_Message, PollMessagesBeforeLoginThrows) {
    RecordProperty("description", "Throws when polling messages before authentication.");
    iggy::ffi::Client *client = GetLoggedOutClient();
    ASSERT_NO_THROW(client->connect());

    ASSERT_THROW(client->poll_messages(make_numeric_identifier(1), make_numeric_identifier(0), 0, "consumer",
                                       make_numeric_identifier(1), "offset", 0, 10, false),
                 std::exception);
    ASSERT_NO_THROW(client->login_user("iggy", "iggy"));
    ASSERT_NO_THROW(client->disconnect());
    ASSERT_THROW(client->poll_messages(make_numeric_identifier(1), make_numeric_identifier(0), 0, "consumer",
                                       make_numeric_identifier(1), "offset", 0, 10, false),
                 std::exception);
}

TEST_F(LowLevelE2E_Message, PollMessagesWithInvalidStreamIdThrows) {
    RecordProperty("description", "Throws when polling messages with an invalid stream identifier.");
    iggy::ffi::Client *client = GetLoggedInClient();

    iggy::ffi::Identifier invalid_id;
    invalid_id.kind   = "invalid";
    invalid_id.length = 0;

    ASSERT_THROW(client->poll_messages(invalid_id, make_numeric_identifier(0), 0, "consumer",
                                       make_numeric_identifier(1), "offset", 0, 10, false),
                 std::exception);
}

TEST_F(LowLevelE2E_Message, PollMessagesFromNonExistentStreamThrows) {
    RecordProperty("description", "Throws when polling messages from a non-existent stream.");
    iggy::ffi::Client *client = GetLoggedInClient();

    ASSERT_THROW(client->poll_messages(make_string_identifier("nonexistent-stream-poll"), make_numeric_identifier(0), 0,
                                       "consumer", make_numeric_identifier(1), "offset", 0, 10, false),
                 std::exception);
}

TEST_F(LowLevelE2E_Message, PollMessagesWithInvalidConsumerKindThrows) {
    RecordProperty("description", "Throws when polling messages with an invalid consumer kind.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    TrackStream(stream.id);
    const std::string topic_name = GetRandomName();
    client->create_topic(make_numeric_identifier(stream.id), topic_name, 1, "none", 0, "never_expire", 0,
                         "server_default");

    ASSERT_THROW(client->poll_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), 0, "invalid",
                                       make_numeric_identifier(1), "offset", 0, 10, false),
                 std::exception);
}

TEST_F(LowLevelE2E_Message, PollMessagesWithInvalidStrategyKindThrows) {
    RecordProperty("description", "Throws when polling messages with an invalid polling strategy kind.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    TrackStream(stream.id);
    const std::string topic_name = GetRandomName();
    client->create_topic(make_numeric_identifier(stream.id), topic_name, 1, "none", 0, "never_expire", 0,
                         "server_default");

    ASSERT_THROW(client->poll_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), 0, "consumer",
                                       make_numeric_identifier(1), "invalid", 0, 10, false),
                 std::exception);
}

TEST_F(LowLevelE2E_Message, PollMessagesCountLessThanAvailable) {
    RecordProperty("description", "Returns only the requested count when fewer messages are requested than available.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    TrackStream(stream.id);
    const std::string topic_name = GetRandomName();
    client->create_topic(make_numeric_identifier(stream.id), topic_name, 1, "none", 0, "never_expire", 0,
                         "server_default");

    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    for (std::uint32_t i = 0; i < 10; i++) {
        auto msg = iggy::ffi::make_message(to_payload("msg-" + std::to_string(i)), rust::Vec<iggy::ffi::HeaderEntry>());
        messages.push_back(std::move(msg));
    }

    client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), "partition_id",
                          partition_id_bytes(0), std::move(messages));

    auto polled = client->poll_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), 0, "consumer",
                                        make_numeric_identifier(1), "offset", 0, 5, false);

    ASSERT_EQ(polled.count, 5u);
    ASSERT_EQ(polled.messages.size(), 5u);
}

TEST_F(LowLevelE2E_Message, PollMessagesWithLargeOffset) {
    RecordProperty("description", "Returns zero messages when polling with an offset beyond available messages.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    TrackStream(stream.id);
    const std::string topic_name = GetRandomName();
    client->create_topic(make_numeric_identifier(stream.id), topic_name, 1, "none", 0, "never_expire", 0,
                         "server_default");

    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    for (std::uint32_t i = 0; i < 5; i++) {
        auto msg = iggy::ffi::make_message(to_payload("msg-" + std::to_string(i)), rust::Vec<iggy::ffi::HeaderEntry>());
        messages.push_back(std::move(msg));
    }

    client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), "partition_id",
                          partition_id_bytes(0), std::move(messages));

    auto polled = client->poll_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), 0, "consumer",
                                        make_numeric_identifier(1), "offset", 999999, 100, false);

    ASSERT_EQ(polled.count, 0u);
    ASSERT_EQ(polled.messages.size(), 0u);
}

TEST_F(LowLevelE2E_Message, PollMessagesFirstStrategy) {
    RecordProperty("description", "Verifies first polling strategy returns messages from the beginning.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    TrackStream(stream.id);
    const std::string topic_name = GetRandomName();
    client->create_topic(make_numeric_identifier(stream.id), topic_name, 1, "none", 0, "never_expire", 0,
                         "server_default");

    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    for (std::uint32_t i = 0; i < 10; i++) {
        auto msg = iggy::ffi::make_message(to_payload("msg-" + std::to_string(i)), rust::Vec<iggy::ffi::HeaderEntry>());
        messages.push_back(std::move(msg));
    }

    client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), "partition_id",
                          partition_id_bytes(0), std::move(messages));

    auto polled = client->poll_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), 0, "consumer",
                                        make_numeric_identifier(1), "first", 0, 3, false);

    ASSERT_EQ(polled.count, 3u);
    ASSERT_EQ(polled.messages.size(), 3u);
    EXPECT_EQ(polled.messages[0].offset, 0u);
    for (std::uint32_t i = 0; i < 3; i++) {
        EXPECT_EQ(polled.messages[i].offset, static_cast<std::uint64_t>(i));
        std::string expected = "msg-" + std::to_string(i);
        std::string actual(polled.messages[i].payload.begin(), polled.messages[i].payload.end());
        EXPECT_EQ(actual, expected) << "Payload mismatch at offset " << i;
    }
}

TEST_F(LowLevelE2E_Message, PollMessagesLastStrategy) {
    RecordProperty("description", "Verifies last polling strategy returns messages from the end.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    TrackStream(stream.id);
    const std::string topic_name = GetRandomName();
    client->create_topic(make_numeric_identifier(stream.id), topic_name, 1, "none", 0, "never_expire", 0,
                         "server_default");

    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    for (std::uint32_t i = 0; i < 10; i++) {
        auto msg = iggy::ffi::make_message(to_payload("msg-" + std::to_string(i)), rust::Vec<iggy::ffi::HeaderEntry>());
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
    for (std::uint32_t i = 0; i < 3; i++) {
        std::string expected = "msg-" + std::to_string(7 + i);
        std::string actual(polled.messages[i].payload.begin(), polled.messages[i].payload.end());
        EXPECT_EQ(actual, expected) << "Payload mismatch at index " << i;
    }
}

TEST_F(LowLevelE2E_Message, PollMessagesNextStrategyNoAutoCommit) {
    RecordProperty("description",
                   "Verifies next strategy without auto-commit returns the same messages on repeated calls.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    TrackStream(stream.id);
    const std::string topic_name = GetRandomName();
    client->create_topic(make_numeric_identifier(stream.id), topic_name, 1, "none", 0, "never_expire", 0,
                         "server_default");

    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    for (std::uint32_t i = 0; i < 5; i++) {
        auto msg = iggy::ffi::make_message(to_payload("msg-" + std::to_string(i)), rust::Vec<iggy::ffi::HeaderEntry>());
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
    for (std::uint32_t i = 0; i < 5; i++) {
        EXPECT_EQ(polled1.messages[i].offset, static_cast<std::uint64_t>(i));
        std::string expected = "msg-" + std::to_string(i);
        std::string actual(polled1.messages[i].payload.begin(), polled1.messages[i].payload.end());
        EXPECT_EQ(actual, expected) << "polled1 payload mismatch at index " << i;
    }
    for (std::uint32_t i = 0; i < 5; i++) {
        EXPECT_EQ(polled2.messages[i].offset, static_cast<std::uint64_t>(i));
        std::string expected = "msg-" + std::to_string(i);
        std::string actual(polled2.messages[i].payload.begin(), polled2.messages[i].payload.end());
        EXPECT_EQ(actual, expected) << "polled2 payload mismatch at index " << i;
    }
}

TEST_F(LowLevelE2E_Message, PollMessagesNextStrategyAutoCommit) {
    RecordProperty("description", "Verifies next strategy with auto-commit advances the offset on subsequent polls.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    TrackStream(stream.id);
    const std::string topic_name = GetRandomName();
    client->create_topic(make_numeric_identifier(stream.id), topic_name, 1, "none", 0, "never_expire", 0,
                         "server_default");

    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    for (std::uint32_t i = 0; i < 10; i++) {
        auto msg = iggy::ffi::make_message(to_payload("msg-" + std::to_string(i)), rust::Vec<iggy::ffi::HeaderEntry>());
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
    for (std::uint32_t i = 0; i < 5; i++) {
        std::string expected1 = "msg-" + std::to_string(i);
        std::string actual1(polled1.messages[i].payload.begin(), polled1.messages[i].payload.end());
        EXPECT_EQ(actual1, expected1) << "polled1 payload mismatch at index " << i;
    }
    for (std::uint32_t i = 0; i < 5; i++) {
        std::string expected2 = "msg-" + std::to_string(5 + i);
        std::string actual2(polled2.messages[i].payload.begin(), polled2.messages[i].payload.end());
        EXPECT_EQ(actual2, expected2) << "polled2 payload mismatch at index " << i;
    }

    auto polled3 = client->poll_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), 0, "consumer",
                                         make_numeric_identifier(1), "next", 0, 5, true);
    ASSERT_EQ(polled3.count, 0u);
}

TEST_F(LowLevelE2E_Message, PollMessagesConsumerIdIndependence) {
    RecordProperty("description", "Verifies different consumer IDs maintain independent offsets.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    TrackStream(stream.id);
    const std::string topic_name = GetRandomName();
    client->create_topic(make_numeric_identifier(stream.id), topic_name, 1, "none", 0, "never_expire", 0,
                         "server_default");

    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    for (std::uint32_t i = 0; i < 5; i++) {
        auto msg = iggy::ffi::make_message(to_payload("msg-" + std::to_string(i)), rust::Vec<iggy::ffi::HeaderEntry>());
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
}

TEST_F(LowLevelE2E_Message, PollMessagesMultipleSendsThenPollOrder) {
    RecordProperty("description", "Verifies message ordering is preserved across multiple send batches.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    TrackStream(stream.id);
    const std::string topic_name = GetRandomName();
    client->create_topic(make_numeric_identifier(stream.id), topic_name, 1, "none", 0, "never_expire", 0,
                         "server_default");

    rust::Vec<iggy::ffi::IggyMessageToSend> batch1;
    for (std::uint32_t i = 0; i < 5; i++) {
        auto msg =
            iggy::ffi::make_message(to_payload("batch1-" + std::to_string(i)), rust::Vec<iggy::ffi::HeaderEntry>());
        batch1.push_back(std::move(msg));
    }
    client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), "partition_id",
                          partition_id_bytes(0), std::move(batch1));

    rust::Vec<iggy::ffi::IggyMessageToSend> batch2;
    for (std::uint32_t i = 0; i < 5; i++) {
        auto msg =
            iggy::ffi::make_message(to_payload("batch2-" + std::to_string(i)), rust::Vec<iggy::ffi::HeaderEntry>());
        batch2.push_back(std::move(msg));
    }
    client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), "partition_id",
                          partition_id_bytes(0), std::move(batch2));

    auto polled = client->poll_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), 0, "consumer",
                                        make_numeric_identifier(1), "offset", 0, 100, false);

    ASSERT_EQ(polled.count, 10u);
    for (std::uint32_t i = 0; i < 10; i++) {
        EXPECT_EQ(polled.messages[i].offset, static_cast<std::uint64_t>(i)) << "Offset mismatch at index " << i;
    }
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
}

TEST_F(LowLevelE2E_Message, PollMessagesMultipleCustomIds) {
    RecordProperty("description", "Verifies multiple messages with distinct custom IDs are all preserved.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    TrackStream(stream.id);
    const std::string topic_name = GetRandomName();
    client->create_topic(make_numeric_identifier(stream.id), topic_name, 1, "none", 0, "never_expire", 0,
                         "server_default");

    const std::uint64_t id_values[] = {100, 200, 300, 400, 500};
    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    for (std::uint32_t i = 0; i < 5; i++) {
        auto msg = iggy::ffi::make_message(to_payload("msg-" + std::to_string(i)), rust::Vec<iggy::ffi::HeaderEntry>());
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
}

TEST_F(LowLevelE2E_Message, PollMessagesAfterStreamDeletedThrows) {
    RecordProperty("description", "Throws when polling messages after the stream has been deleted.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    TrackStream(stream.id);
    const std::string topic_name = GetRandomName();
    client->create_topic(make_numeric_identifier(stream.id), topic_name, 1, "none", 0, "never_expire", 0,
                         "server_default");

    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    auto msg = iggy::ffi::make_message(to_payload("test"), rust::Vec<iggy::ffi::HeaderEntry>());
    messages.push_back(std::move(msg));

    client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), "partition_id",
                          partition_id_bytes(0), std::move(messages));

    std::uint32_t saved_stream_id = stream.id;
    client->delete_stream(make_numeric_identifier(saved_stream_id));
    ForgetTrackedStream(saved_stream_id);

    ASSERT_THROW(client->poll_messages(make_numeric_identifier(saved_stream_id), make_numeric_identifier(0), 0,
                                       "consumer", make_numeric_identifier(1), "offset", 0, 10, false),
                 std::exception);
}

TEST_F(LowLevelE2E_Message, PollMessagesWithInvalidPartitionIdThrows) {
    RecordProperty("description", "Throws when polling with a non-existent partition ID.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    TrackStream(stream.id);
    const std::string topic_name = GetRandomName();
    client->create_topic(make_numeric_identifier(stream.id), topic_name, 1, "none", 0, "never_expire", 0,
                         "server_default");

    ASSERT_THROW(client->poll_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), 9999, "consumer",
                                       make_numeric_identifier(1), "offset", 0, 10, false),
                 std::exception);
}

TEST_F(LowLevelE2E_Message, PollMessagesWithCountZeroThrows) {
    RecordProperty("description", "Throws when polling with count=0.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    TrackStream(stream.id);
    const std::string topic_name = GetRandomName();
    client->create_topic(make_numeric_identifier(stream.id), topic_name, 1, "none", 0, "never_expire", 0,
                         "server_default");

    ASSERT_THROW(client->poll_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), 0, "consumer",
                                       make_numeric_identifier(1), "offset", 0, 0, false),
                 std::exception);
}

TEST_F(LowLevelE2E_Message, PollMessagesWithoutSpecifyingPartition) {
    RecordProperty("description",
                   "Verifies polling with partition_id=u32::MAX defaults to partition 0 and returns messages.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    TrackStream(stream.id);
    const std::string topic_name = GetRandomName();
    client->create_topic(make_numeric_identifier(stream.id), topic_name, 1, "none", 0, "never_expire", 0,
                         "server_default");

    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    for (std::uint32_t i = 0; i < 5; i++) {
        auto msg = iggy::ffi::make_message(to_payload("msg-" + std::to_string(i)), rust::Vec<iggy::ffi::HeaderEntry>());
        messages.push_back(std::move(msg));
    }
    client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), "partition_id",
                          partition_id_bytes(0), std::move(messages));

    auto polled = client->poll_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), UINT32_MAX,
                                        "consumer", make_numeric_identifier(1), "offset", 0, 100, false);

    // The Rust side maps UINT32_MAX to None, so the server picks a partition. With a single
    // partition topic that should always be partition 0.
    ASSERT_EQ(polled.partition_id, 0u) << "u32::MAX sentinel did not map to None — partition_id sentinel regression?";
    ASSERT_EQ(polled.count, 5u);
    ASSERT_EQ(polled.messages.size(), 5u);
    for (std::uint32_t i = 0; i < 5; i++) {
        std::string expected = "msg-" + std::to_string(i);
        std::string actual(polled.messages[i].payload.begin(), polled.messages[i].payload.end());
        EXPECT_EQ(actual, expected) << "Payload mismatch at index " << i;
    }
}

TEST_F(LowLevelE2E_Message, PollMessagesTimestampStrategy) {
    RecordProperty("description",
                   "Verifies timestamp polling strategy returns messages with timestamp >= the specified value.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    TrackStream(stream.id);
    const std::string topic_name = GetRandomName();
    client->create_topic(make_numeric_identifier(stream.id), topic_name, 1, "none", 0, "never_expire", 0,
                         "server_default");

    rust::Vec<iggy::ffi::IggyMessageToSend> batch1;
    for (std::uint32_t i = 0; i < 5; i++) {
        auto msg =
            iggy::ffi::make_message(to_payload("batch1-" + std::to_string(i)), rust::Vec<iggy::ffi::HeaderEntry>());
        batch1.push_back(std::move(msg));
    }
    client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), "partition_id",
                          partition_id_bytes(0), std::move(batch1));

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    rust::Vec<iggy::ffi::IggyMessageToSend> batch2;
    for (std::uint32_t i = 0; i < 5; i++) {
        auto msg =
            iggy::ffi::make_message(to_payload("batch2-" + std::to_string(i)), rust::Vec<iggy::ffi::HeaderEntry>());
        batch2.push_back(std::move(msg));
    }
    client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), "partition_id",
                          partition_id_bytes(0), std::move(batch2));

    auto all = client->poll_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), 0, "consumer",
                                     make_numeric_identifier(1), "offset", 0, 100, false);
    ASSERT_EQ(all.count, 10u);

    // IggyTimestamp::now() is microsecond-resolution and we slept 100ms between batches; a gap
    // smaller than half that window means the test has degraded into a tautology on busy CI.
    constexpr std::uint64_t kMinTimestampGapMicros = 50'000;
    std::uint64_t batch1_timestamp                 = all.messages[0].timestamp;
    std::uint64_t batch2_timestamp                 = all.messages[5].timestamp;
    ASSERT_GT(batch2_timestamp, batch1_timestamp);
    ASSERT_GE(batch2_timestamp - batch1_timestamp, kMinTimestampGapMicros)
        << "Timestamp gap collapsed (" << (batch2_timestamp - batch1_timestamp)
        << "us) — test no longer exercises timestamp filtering";

    auto polled = client->poll_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), 0, "consumer",
                                        make_numeric_identifier(2), "timestamp", batch2_timestamp, 100, false);

    ASSERT_GE(polled.count, 5u);
    // The server contract is `timestamp >= polling_strategy_value`. If a batch1 message lands on
    // exactly the same microsecond as batch2's first message, the count can legitimately exceed 5,
    // so verify by prefix rather than indexing each message against `batch2-N`.
    for (std::size_t i = 0; i < polled.messages.size(); i++) {
        EXPECT_GE(polled.messages[i].timestamp, batch2_timestamp)
            << "Message at index " << i << " has earlier timestamp";
        std::string actual(polled.messages[i].payload.begin(), polled.messages[i].payload.end());
        EXPECT_TRUE(actual.rfind("batch1-", 0) == 0 || actual.rfind("batch2-", 0) == 0)
            << "Polled message at index " << i << " has unexpected payload: " << actual;
    }
}

TEST_F(LowLevelE2E_Message, PollMessagesMonotonicOffsets) {
    RecordProperty("description",
                   "Verifies offsets are monotonically increasing and continuous across multiple polls.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    TrackStream(stream.id);
    const std::string topic_name = GetRandomName();
    client->create_topic(make_numeric_identifier(stream.id), topic_name, 1, "none", 0, "never_expire", 0,
                         "server_default");

    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    for (std::uint32_t i = 0; i < 20; i++) {
        auto msg =
            iggy::ffi::make_message(to_payload("mono-" + std::to_string(i)), rust::Vec<iggy::ffi::HeaderEntry>());
        messages.push_back(std::move(msg));
    }
    client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), "partition_id",
                          partition_id_bytes(0), std::move(messages));

    std::uint64_t expected_offset = 0;
    for (int chunk = 0; chunk < 4; chunk++) {
        auto polled =
            client->poll_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), 0, "consumer",
                                  make_numeric_identifier(1), "offset", expected_offset, 5, false);

        ASSERT_EQ(polled.count, 5u) << "Chunk " << chunk;
        ASSERT_EQ(polled.messages.size(), 5u) << "Chunk " << chunk;

        for (std::size_t i = 0; i < polled.messages.size(); i++) {
            EXPECT_EQ(polled.messages[i].offset, expected_offset) << "Chunk " << chunk << " index " << i;
            expected_offset++;
        }
    }

    ASSERT_EQ(expected_offset, 20u);
}

TEST_F(LowLevelE2E_Message, SendMessagesLargeBatch) {
    RecordProperty("description", "Verifies sending a large batch of 1000 messages succeeds and all are retrievable.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    TrackStream(stream.id);
    const std::string topic_name = GetRandomName();
    client->create_topic(make_numeric_identifier(stream.id), topic_name, 1, "none", 0, "never_expire", 0,
                         "server_default");

    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    for (std::uint32_t i = 0; i < 1000; i++) {
        auto msg =
            iggy::ffi::make_message(to_payload("batch-msg-" + std::to_string(i)), rust::Vec<iggy::ffi::HeaderEntry>());
        messages.push_back(std::move(msg));
    }

    ASSERT_NO_THROW(client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0),
                                          "partition_id", partition_id_bytes(0), std::move(messages)));

    auto polled = client->poll_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), 0, "consumer",
                                        make_numeric_identifier(1), "offset", 0, 1000, false);

    ASSERT_EQ(polled.count, 1000u);
    ASSERT_EQ(polled.messages.size(), 1000u);
    EXPECT_EQ(polled.messages[0].offset, 0u);
    EXPECT_EQ(polled.messages[999].offset, 999u);
}

TEST_F(LowLevelE2E_Message, SendMessagesWithInvalidTopicIdThrows) {
    RecordProperty("description", "Throws when sending messages with an invalid topic identifier.");
    iggy::ffi::Client *client = GetLoggedInClient();

    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    auto msg = iggy::ffi::make_message(to_payload("test"), rust::Vec<iggy::ffi::HeaderEntry>());
    messages.push_back(std::move(msg));

    iggy::ffi::Identifier invalid_id;
    invalid_id.kind   = "invalid";
    invalid_id.length = 0;

    ASSERT_THROW(client->send_messages(make_numeric_identifier(1), invalid_id, "partition_id", partition_id_bytes(0),
                                       std::move(messages)),
                 std::exception);
}

TEST_F(LowLevelE2E_Message, PollMessagesWithInvalidTopicIdThrows) {
    RecordProperty("description", "Throws when polling messages with an invalid topic identifier.");
    iggy::ffi::Client *client = GetLoggedInClient();

    iggy::ffi::Identifier invalid_id;
    invalid_id.kind   = "invalid";
    invalid_id.length = 0;

    ASSERT_THROW(client->poll_messages(make_numeric_identifier(1), invalid_id, 0, "consumer",
                                       make_numeric_identifier(1), "offset", 0, 10, false),
                 std::exception);
}

TEST_F(LowLevelE2E_Message, PollMessagesWithInvalidConsumerIdThrows) {
    RecordProperty("description", "Throws when polling messages with an invalid consumer identifier.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    TrackStream(stream.id);
    const std::string topic_name = GetRandomName();
    client->create_topic(make_numeric_identifier(stream.id), topic_name, 1, "none", 0, "never_expire", 0,
                         "server_default");

    iggy::ffi::Identifier invalid_id;
    invalid_id.kind   = "invalid";
    invalid_id.length = 0;

    ASSERT_THROW(client->poll_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), 0, "consumer",
                                       invalid_id, "offset", 0, 10, false),
                 std::exception);
}

TEST_F(LowLevelE2E_Message, ConsumerGroupCreateJoinAndPollMessages) {
    RecordProperty("description",
                   "Creates a consumer group, joins it, sends messages, and polls them using consumer_group kind.");
    const std::string stream_name = GetRandomName();
    iggy::ffi::Client *client     = GetLoggedInClient();

    client->create_stream(stream_name);
    auto stream = client->get_stream(make_string_identifier(stream_name));
    TrackStream(stream.id);
    const std::string topic_name = GetRandomName();
    client->create_topic(make_numeric_identifier(stream.id), topic_name, 1, "none", 0, "never_expire", 0,
                         "server_default");

    const std::string group_name = GetRandomName();
    auto group =
        client->create_consumer_group(make_numeric_identifier(stream.id), make_numeric_identifier(0), group_name);
    ASSERT_EQ(group.members_count, 0u);

    ASSERT_NO_THROW(client->join_consumer_group(make_numeric_identifier(stream.id), make_numeric_identifier(0),
                                                make_numeric_identifier(group.id)));

    auto group_after_join = client->get_consumer_group(make_numeric_identifier(stream.id), make_numeric_identifier(0),
                                                       make_numeric_identifier(group.id));
    ASSERT_EQ(group_after_join.members_count, 1u);

    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    for (std::uint32_t i = 0; i < 10; i++) {
        auto msg =
            iggy::ffi::make_message(to_payload("cg-msg-" + std::to_string(i)), rust::Vec<iggy::ffi::HeaderEntry>());
        messages.push_back(std::move(msg));
    }
    client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), "partition_id",
                          partition_id_bytes(0), std::move(messages));

    auto polled = client->poll_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), 0,
                                        "consumer_group", make_numeric_identifier(group.id), "offset", 0, 100, false);

    ASSERT_EQ(polled.count, 10u);
    ASSERT_EQ(polled.messages.size(), 10u);
    for (std::uint32_t i = 0; i < 10; i++) {
        std::string expected = "cg-msg-" + std::to_string(i);
        std::string actual(polled.messages[i].payload.begin(), polled.messages[i].payload.end());
        EXPECT_EQ(actual, expected) << "Payload mismatch at offset " << i;
    }

    ASSERT_NO_THROW(client->leave_consumer_group(make_numeric_identifier(stream.id), make_numeric_identifier(0),
                                                 make_numeric_identifier(group.id)));

    auto group_after_leave = client->get_consumer_group(make_numeric_identifier(stream.id), make_numeric_identifier(0),
                                                        make_numeric_identifier(group.id));
    ASSERT_EQ(group_after_leave.members_count, 0u);
}
