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
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "lib.rs.h"
#include "tests/e2e/test_helpers.hpp"

class E2E_Message : public E2ETestFixture {};

namespace {

bool has_std_header(const std::vector<iggy::HeaderEntry> &headers,
                    const iggy::HeaderKind key_kind,
                    const std::string_view key_value,
                    const iggy::HeaderKind value_kind,
                    const std::vector<std::uint8_t> &value_value) {
    for (const auto &header : headers) {
        const auto &key_bytes = header.Key().Value();
        const std::string_view key(key_bytes.empty() ? nullptr : reinterpret_cast<const char *>(key_bytes.data()),
                                   key_bytes.size());
        if (header.Key().Kind() == key_kind && header.Value().Kind() == value_kind && key == key_value &&
            header.Value().Value() == value_value) {
            return true;
        }
    }
    return false;
}

}  // namespace

TEST_F(E2E_Message, SendAndPollMessagesRoundTrip) {
    RecordProperty("description", "Sends 10 messages and polls them back, verifying count, offsets, and payloads.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    client.CreateStream(stream_name);
    const auto stream = client.GetStream(iggy::Identifier::String(stream_name));
    TrackStream(stream.Id());
    const std::string topic_name = GetRandomName();
    iggy::TopicCreateOptions topic_options;
    topic_options.SetPartitionsCount(1)
        .SetCompressionAlgorithm(iggy::CompressionAlgorithm::None())
        .SetMessageExpiry(iggy::Expiry::NeverExpire());
    client.CreateTopic(iggy::Identifier::Numeric(stream.Id()), topic_name, topic_options);

    std::vector<iggy::IggyMessageToSend> messages;
    for (std::uint32_t i = 0; i < 10; i++) {
        auto msg =
            iggy::IggyMessageToSend::Create("test message " + std::to_string(i), std::vector<iggy::HeaderEntry>());
        messages.push_back(std::move(msg));
    }

    std::optional<iggy::SendMessagesResponse> sent;
    ASSERT_NO_THROW(sent = client.SendMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0),
                                               iggy::Partitioning::PartitionId(0), messages));

    ASSERT_EQ(sent->Confirmations().size(), 1u)
        << "The VSR server reports the written partition's offsets, so a single-partition send "
        << "must carry exactly one confirmation";
    EXPECT_EQ(sent->Confirmations().front().PartitionId(), 0u);

    auto polled = client.PollMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0), 0,
                                      iggy::Consumer::Single(iggy::Identifier::Numeric(1)),
                                      iggy::PollingStrategy::Offset(0), 100, false);

    ASSERT_EQ(polled.PartitionId(), 0u) << "Polled partition_id mismatches the partition we sent to";
    ASSERT_EQ(polled.Count(), 10u);
    ASSERT_EQ(polled.Messages().size(), 10u);
    for (std::uint32_t i = 0; i < 10; i++) {
        ASSERT_EQ(polled.Messages()[i].Offset(), static_cast<std::uint64_t>(i));
        std::string expected = "test message " + std::to_string(i);
        std::string actual(polled.Messages()[i].Payload().begin(), polled.Messages()[i].Payload().end());
        ASSERT_EQ(actual, expected) << "Payload mismatch at offset " << i;
    }
}

TEST_F(E2E_Message, PollMessagesVerifyMessageIds) {
    RecordProperty("description", "Verifies that polled message IDs match the sent IDs.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    client.CreateStream(stream_name);
    const auto stream = client.GetStream(iggy::Identifier::String(stream_name));
    TrackStream(stream.Id());
    const std::string topic_name = GetRandomName();
    iggy::TopicCreateOptions topic_options;
    topic_options.SetPartitionsCount(1)
        .SetCompressionAlgorithm(iggy::CompressionAlgorithm::None())
        .SetMessageExpiry(iggy::Expiry::NeverExpire());
    client.CreateTopic(iggy::Identifier::Numeric(stream.Id()), topic_name, topic_options);

    std::vector<iggy::IggyMessageToSend> messages;
    auto msg = iggy::IggyMessageToSend::Create("id-test-message", std::vector<iggy::HeaderEntry>(),
                                               static_cast<absl::uint128>(42));
    messages.push_back(std::move(msg));

    client.SendMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0),
                        iggy::Partitioning::PartitionId(0), messages);

    auto polled = client.PollMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0), 0,
                                      iggy::Consumer::Single(iggy::Identifier::Numeric(1)),
                                      iggy::PollingStrategy::Offset(0), 100, false);

    ASSERT_EQ(polled.Messages().size(), 1u);
    ASSERT_EQ(polled.Messages()[0].Id(), static_cast<absl::uint128>(42));
}

TEST_F(E2E_Message, PollMessagesFromEmptyPartition) {
    RecordProperty("description", "Verifies polling from an empty partition returns zero messages.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    client.CreateStream(stream_name);
    const auto stream = client.GetStream(iggy::Identifier::String(stream_name));
    TrackStream(stream.Id());
    const std::string topic_name = GetRandomName();
    iggy::TopicCreateOptions topic_options;
    topic_options.SetPartitionsCount(1)
        .SetCompressionAlgorithm(iggy::CompressionAlgorithm::None())
        .SetMessageExpiry(iggy::Expiry::NeverExpire());
    client.CreateTopic(iggy::Identifier::Numeric(stream.Id()), topic_name, topic_options);

    auto polled = client.PollMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0), 0,
                                      iggy::Consumer::Single(iggy::Identifier::Numeric(1)),
                                      iggy::PollingStrategy::Offset(0), 100, false);

    ASSERT_EQ(polled.Count(), 0u);
    ASSERT_EQ(polled.Messages().size(), 0u);
}

TEST_F(E2E_Message, SendMessagesBeforeLoginThrows) {
    RecordProperty("description", "Verifies send_messages throws when not authenticated.");
    auto client = GetLoggedOutHighLevelClient();
    ASSERT_NO_THROW(client.Connect());

    std::vector<iggy::IggyMessageToSend> messages;
    messages.push_back(iggy::IggyMessageToSend::Create("should-fail", {}));
    ASSERT_THROW(client.SendMessages(iggy::Identifier::Numeric(1), iggy::Identifier::Numeric(1),
                                     iggy::Partitioning::PartitionId(0), messages),
                 std::exception);
    ASSERT_NO_THROW(client.Login("iggy", "iggy"));
    ASSERT_NO_THROW(client.Disconnect());

    std::vector<iggy::IggyMessageToSend> disconnected_messages;
    disconnected_messages.push_back(iggy::IggyMessageToSend::Create("should-still-fail", {}));
    ASSERT_THROW(client.SendMessages(iggy::Identifier::Numeric(1), iggy::Identifier::Numeric(1),
                                     iggy::Partitioning::PartitionId(0), disconnected_messages),
                 std::exception);
}

TEST_F(E2E_Message, SendMessagesToNonExistentStream) {
    RecordProperty("description", "Throws when sending messages to a non-existent stream.");
    auto client = GetLoggedInHighLevelClient();

    std::vector<iggy::IggyMessageToSend> messages;
    messages.push_back(iggy::IggyMessageToSend::Create("test", {}));

    ASSERT_THROW(client.SendMessages(iggy::Identifier::String("nonexistent-stream-12345"), iggy::Identifier::Numeric(0),
                                     iggy::Partitioning::PartitionId(0), messages),
                 std::exception);
}

TEST_F(E2E_Message, SendMessagesToSpecificPartitionVerified) {
    RecordProperty("description",
                   "Verifies messages sent to a specific partition are only retrievable from that partition.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    client.CreateStream(stream_name);
    const auto stream = client.GetStream(iggy::Identifier::String(stream_name));
    TrackStream(stream.Id());
    const std::string topic_name = GetRandomName();
    iggy::TopicCreateOptions topic_options;
    topic_options.SetPartitionsCount(3)
        .SetCompressionAlgorithm(iggy::CompressionAlgorithm::None())
        .SetMessageExpiry(iggy::Expiry::NeverExpire());
    client.CreateTopic(iggy::Identifier::Numeric(stream.Id()), topic_name, topic_options);

    std::vector<iggy::IggyMessageToSend> messages;
    for (std::uint32_t i = 0; i < 5; i++) {
        auto msg =
            iggy::IggyMessageToSend::Create("partition-test-" + std::to_string(i), std::vector<iggy::HeaderEntry>());
        messages.push_back(std::move(msg));
    }

    client.SendMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0),
                        iggy::Partitioning::PartitionId(0), messages);

    auto polled_part0 = client.PollMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0), 0,
                                            iggy::Consumer::Single(iggy::Identifier::Numeric(1)),
                                            iggy::PollingStrategy::Offset(0), 100, false);
    ASSERT_EQ(polled_part0.PartitionId(), 0u);
    ASSERT_EQ(polled_part0.Count(), 5u);

    auto polled_part1 = client.PollMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0), 1,
                                            iggy::Consumer::Single(iggy::Identifier::Numeric(1)),
                                            iggy::PollingStrategy::Offset(0), 100, false);
    ASSERT_EQ(polled_part1.PartitionId(), 1u);
    ASSERT_EQ(polled_part1.Count(), 0u);
}

TEST_F(E2E_Message, SendEmptyMessageVectorThrows) {
    RecordProperty("description", "Throws when sending an empty message vector.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    client.CreateStream(stream_name);
    const auto stream = client.GetStream(iggy::Identifier::String(stream_name));
    TrackStream(stream.Id());
    const std::string topic_name = GetRandomName();
    iggy::TopicCreateOptions topic_options;
    topic_options.SetPartitionsCount(1)
        .SetCompressionAlgorithm(iggy::CompressionAlgorithm::None())
        .SetMessageExpiry(iggy::Expiry::NeverExpire());
    client.CreateTopic(iggy::Identifier::Numeric(stream.Id()), topic_name, topic_options);

    std::vector<iggy::IggyMessageToSend> empty_messages;

    ASSERT_THROW(client.SendMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0),
                                     iggy::Partitioning::PartitionId(0), empty_messages),
                 std::exception);
}

TEST_F(E2E_Message, SendMessageWithEmptyPayloadThrows) {
    RecordProperty("description", "Throws when sending a message with an empty payload.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    client.CreateStream(stream_name);
    const auto stream = client.GetStream(iggy::Identifier::String(stream_name));
    TrackStream(stream.Id());
    const std::string topic_name = GetRandomName();
    iggy::TopicCreateOptions topic_options;
    topic_options.SetPartitionsCount(1)
        .SetCompressionAlgorithm(iggy::CompressionAlgorithm::None())
        .SetMessageExpiry(iggy::Expiry::NeverExpire());
    client.CreateTopic(iggy::Identifier::Numeric(stream.Id()), topic_name, topic_options);

    std::vector<iggy::IggyMessageToSend> messages;
    auto msg = iggy::IggyMessageToSend::Create("", std::vector<iggy::HeaderEntry>());
    messages.push_back(std::move(msg));

    ASSERT_THROW(client.SendMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0),
                                     iggy::Partitioning::PartitionId(0), messages),
                 std::exception);
}

TEST_F(E2E_Message, SendMessageWithOversizedPayloadThrows) {
    RecordProperty("description", "Throws when sending a message exceeding maximum payload size.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    client.CreateStream(stream_name);
    const auto stream = client.GetStream(iggy::Identifier::String(stream_name));
    TrackStream(stream.Id());
    const std::string topic_name = GetRandomName();
    iggy::TopicCreateOptions topic_options;
    topic_options.SetPartitionsCount(1)
        .SetCompressionAlgorithm(iggy::CompressionAlgorithm::None())
        .SetMessageExpiry(iggy::Expiry::NeverExpire());
    client.CreateTopic(iggy::Identifier::Numeric(stream.Id()), topic_name, topic_options);

    // Build a payload one byte over the SDK's max payload size (64 MB).
    constexpr std::uint32_t kOversizedPayloadBytes = 64'000'001u;
    const std::string oversized_payload(kOversizedPayloadBytes, 'A');

    std::vector<iggy::IggyMessageToSend> messages;
    auto msg = iggy::IggyMessageToSend::Create(oversized_payload, std::vector<iggy::HeaderEntry>());
    messages.push_back(std::move(msg));

    ASSERT_THROW(client.SendMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0),
                                     iggy::Partitioning::PartitionId(0), messages),
                 std::exception);
}

TEST_F(E2E_Message, SendMessagesPreservesOrder) {
    RecordProperty("description", "Verifies messages are stored and retrieved in the order they were sent.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    client.CreateStream(stream_name);
    const auto stream = client.GetStream(iggy::Identifier::String(stream_name));
    TrackStream(stream.Id());
    const std::string topic_name = GetRandomName();
    iggy::TopicCreateOptions topic_options;
    topic_options.SetPartitionsCount(1)
        .SetCompressionAlgorithm(iggy::CompressionAlgorithm::None())
        .SetMessageExpiry(iggy::Expiry::NeverExpire());
    client.CreateTopic(iggy::Identifier::Numeric(stream.Id()), topic_name, topic_options);

    std::vector<iggy::IggyMessageToSend> messages;
    for (std::uint32_t i = 0; i < 50; i++) {
        auto msg = iggy::IggyMessageToSend::Create("order-" + std::to_string(i), std::vector<iggy::HeaderEntry>());
        messages.push_back(std::move(msg));
    }

    client.SendMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0),
                        iggy::Partitioning::PartitionId(0), messages);

    auto polled = client.PollMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0), 0,
                                      iggy::Consumer::Single(iggy::Identifier::Numeric(1)),
                                      iggy::PollingStrategy::Offset(0), 100, false);

    ASSERT_EQ(polled.Count(), 50u);
    for (std::uint32_t i = 0; i < 50; i++) {
        ASSERT_EQ(polled.Messages()[i].Offset(), static_cast<std::uint64_t>(i));
        std::string expected = "order-" + std::to_string(i);
        std::string actual(polled.Messages()[i].Payload().begin(), polled.Messages()[i].Payload().end());
        EXPECT_EQ(actual, expected) << "Payload mismatch at offset " << i;
    }
}

TEST_F(E2E_Message, SendMessagesWithDuplicateIds) {
    RecordProperty("description", "Verifies sending multiple messages with the same ID succeeds.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    client.CreateStream(stream_name);
    const auto stream = client.GetStream(iggy::Identifier::String(stream_name));
    TrackStream(stream.Id());
    const std::string topic_name = GetRandomName();
    iggy::TopicCreateOptions topic_options;
    topic_options.SetPartitionsCount(1)
        .SetCompressionAlgorithm(iggy::CompressionAlgorithm::None())
        .SetMessageExpiry(iggy::Expiry::NeverExpire());
    client.CreateTopic(iggy::Identifier::Numeric(stream.Id()), topic_name, topic_options);

    std::vector<iggy::IggyMessageToSend> messages;
    for (std::uint32_t i = 0; i < 3; i++) {
        messages.push_back(iggy::IggyMessageToSend::Create(
            "dup-id-msg-" + std::to_string(i), std::vector<iggy::HeaderEntry>(), static_cast<absl::uint128>(99)));
    }

    ASSERT_NO_THROW(client.SendMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0),
                                        iggy::Partitioning::PartitionId(0), messages));

    auto polled = client.PollMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0), 0,
                                      iggy::Consumer::Single(iggy::Identifier::Numeric(1)),
                                      iggy::PollingStrategy::Offset(0), 100, false);

    ASSERT_EQ(polled.Count(), 3u);
    for (const auto &message : polled.Messages()) {
        EXPECT_EQ(message.Id(), static_cast<absl::uint128>(99));
    }
}

TEST_F(E2E_Message, SendMessagesWithVariousPayloads) {
    RecordProperty("description",
                   "Verifies various payload types including null bytes, UTF-8, and binary data are preserved.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    client.CreateStream(stream_name);
    const auto stream = client.GetStream(iggy::Identifier::String(stream_name));
    TrackStream(stream.Id());
    const std::string topic_name = GetRandomName();
    iggy::TopicCreateOptions topic_options;
    topic_options.SetPartitionsCount(1)
        .SetCompressionAlgorithm(iggy::CompressionAlgorithm::None())
        .SetMessageExpiry(iggy::Expiry::NeverExpire());
    client.CreateTopic(iggy::Identifier::Numeric(stream.Id()), topic_name, topic_options);

    const std::string payload_null{'\x00', '\x01', '\x00', '\xFF'};

    const std::string payload_binary{'\xDE', '\xAD', '\xBE', '\xEF'};

    std::vector<iggy::IggyMessageToSend> messages;

    auto msg0 = iggy::IggyMessageToSend::Create("simple ascii", std::vector<iggy::HeaderEntry>());
    messages.push_back(std::move(msg0));

    auto msg1 = iggy::IggyMessageToSend::Create(payload_null, std::vector<iggy::HeaderEntry>());
    messages.push_back(std::move(msg1));

    auto msg2 = iggy::IggyMessageToSend::Create("héllo wörld", std::vector<iggy::HeaderEntry>());
    messages.push_back(std::move(msg2));

    auto msg3 = iggy::IggyMessageToSend::Create(payload_binary, std::vector<iggy::HeaderEntry>());
    messages.push_back(std::move(msg3));

    client.SendMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0),
                        iggy::Partitioning::PartitionId(0), messages);

    auto polled = client.PollMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0), 0,
                                      iggy::Consumer::Single(iggy::Identifier::Numeric(1)),
                                      iggy::PollingStrategy::Offset(0), 100, false);

    ASSERT_EQ(polled.Count(), 4u);

    std::string ascii_actual(polled.Messages()[0].Payload().begin(), polled.Messages()[0].Payload().end());
    EXPECT_EQ(ascii_actual, "simple ascii");

    ASSERT_EQ(polled.Messages()[1].Payload().size(), 4u);
    EXPECT_EQ(polled.Messages()[1].Payload()[0], 0x00);
    EXPECT_EQ(polled.Messages()[1].Payload()[1], 0x01);
    EXPECT_EQ(polled.Messages()[1].Payload()[2], 0x00);
    EXPECT_EQ(polled.Messages()[1].Payload()[3], 0xFF);

    std::string utf8_actual(polled.Messages()[2].Payload().begin(), polled.Messages()[2].Payload().end());
    EXPECT_EQ(utf8_actual, "héllo wörld");

    ASSERT_EQ(polled.Messages()[3].Payload().size(), 4u);
    EXPECT_EQ(polled.Messages()[3].Payload()[0], 0xDE);
    EXPECT_EQ(polled.Messages()[3].Payload()[1], 0xAD);
    EXPECT_EQ(polled.Messages()[3].Payload()[2], 0xBE);
    EXPECT_EQ(polled.Messages()[3].Payload()[3], 0xEF);
}

TEST_F(E2E_Message, SendAndPollMessageWithTypedHeadersRoundTrip) {
    RecordProperty(
        "description",
        "Sends one message per typed header kind and verifies payload, IDs, header contents, and encoded size.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    client.CreateStream(stream_name);
    const auto stream = client.GetStream(iggy::Identifier::String(stream_name));
    TrackStream(stream.Id());
    const std::string topic_name = GetRandomName();
    iggy::TopicCreateOptions topic_options;
    topic_options.SetPartitionsCount(1)
        .SetCompressionAlgorithm(iggy::CompressionAlgorithm::None())
        .SetMessageExpiry(iggy::Expiry::NeverExpire());
    client.CreateTopic(iggy::Identifier::Numeric(stream.Id()), topic_name, topic_options);

    struct ExpectedHeaderMessage {
        const char *key;
        iggy::HeaderKind value_kind;
        const std::vector<std::uint8_t> *value;
    };

    const std::vector<std::uint8_t> raw_value{0xDE, 0xAD, 0xBE, 0xEF};      // raw bytes 0xDEADBEEF
    const std::vector<std::uint8_t> string_value{'h', 'e', 'l', 'l', 'o'};  // UTF-8 string "hello"
    const std::vector<std::uint8_t> bool_value{0x01};                       // bool true
    const std::vector<std::uint8_t> int8_value{0xFB};                       // int8 -5
    const std::vector<std::uint8_t> int16_value{0x2E, 0xFB};                // int16 -1234
    const std::vector<std::uint8_t> int32_value{0xEB, 0x32, 0xA4, 0xF8};    // int32 -123456789
    const std::vector<std::uint8_t> int64_value{0x79, 0x29, 0xED, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF};  // int64 -1234567
    const std::vector<std::uint8_t> int128_value{
        0x00, 0xFF, 0xEE, 0xDD, 0xCC, 0xBB, 0xAA, 0x99,
        0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11};                   // int128 0x112233445566778899AABBCCDDEEFF00
    const std::vector<std::uint8_t> uint8_value{0xFA};                     // uint8 250
    const std::vector<std::uint8_t> uint16_value{0xD2, 0x04};              // uint16 1234
    const std::vector<std::uint8_t> uint32_value{0x78, 0x56, 0x34, 0x12};  // uint32 0x12345678
    const std::vector<std::uint8_t> uint64_value{0x88, 0x77, 0x66, 0x55,
                                                 0x44, 0x33, 0x22, 0x11};  // uint64 0x1122334455667788
    const std::vector<std::uint8_t> uint128_value{
        0x10, 0x32, 0x54, 0x76, 0x98, 0xBA, 0xDC, 0xFE,
        0xEF, 0xCD, 0xAB, 0x89, 0x67, 0x45, 0x23, 0x01};  // uint128 0x0123456789ABCDEFFEDCBA9876543210
    const std::vector<std::uint8_t> float32_value{0x00, 0x00, 0x80, 0x3F};                          // float32 1.0
    const std::vector<std::uint8_t> float64_value{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F};  // float64 1.0
    const ExpectedHeaderMessage expected_messages[] = {
        {"raw", iggy::HeaderKind::Raw, &raw_value},
        {"string", iggy::HeaderKind::String, &string_value},
        {"bool", iggy::HeaderKind::Bool, &bool_value},
        {"int8", iggy::HeaderKind::Int8, &int8_value},
        {"int16", iggy::HeaderKind::Int16, &int16_value},
        {"int32", iggy::HeaderKind::Int32, &int32_value},
        {"int64", iggy::HeaderKind::Int64, &int64_value},
        {"int128", iggy::HeaderKind::Int128, &int128_value},
        {"uint8", iggy::HeaderKind::Uint8, &uint8_value},
        {"uint16", iggy::HeaderKind::Uint16, &uint16_value},
        {"uint32", iggy::HeaderKind::Uint32, &uint32_value},
        {"uint64", iggy::HeaderKind::Uint64, &uint64_value},
        {"uint128", iggy::HeaderKind::Uint128, &uint128_value},
        {"float32", iggy::HeaderKind::Float32, &float32_value},
        {"float64", iggy::HeaderKind::Float64, &float64_value},
    };
    std::vector<iggy::IggyMessageToSend> messages;
    for (const auto &expected : expected_messages) {
        std::vector<iggy::HeaderEntry> headers;
        headers.push_back(iggy::HeaderEntry::Create(iggy::HeaderField::Create(iggy::HeaderKind::String, expected.key),
                                                    iggy::HeaderField::Create(expected.value_kind, *expected.value)));

        messages.push_back(iggy::IggyMessageToSend::Create(std::string("payload-") + expected.key, std::move(headers)));
    }

    ASSERT_NO_THROW(client.SendMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0),
                                        iggy::Partitioning::PartitionId(0), messages));

    auto polled = client.PollMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0), 0,
                                      iggy::Consumer::Single(iggy::Identifier::Numeric(1)),
                                      iggy::PollingStrategy::Offset(0), 100, false);

    constexpr std::size_t expected_message_count = sizeof(expected_messages) / sizeof(expected_messages[0]);
    ASSERT_EQ(polled.Count(), expected_message_count);
    ASSERT_EQ(polled.Messages().size(), expected_message_count);
    for (std::size_t i = 0; i < expected_message_count; ++i) {
        const auto &expected       = expected_messages[i];
        const auto &polled_message = polled.Messages()[i];

        EXPECT_EQ(std::string(polled_message.Payload().begin(), polled_message.Payload().end()),
                  std::string("payload-") + expected.key);
        ASSERT_EQ(polled_message.UserHeaders().size(), 1u);
        EXPECT_EQ(polled_message.UserHeadersLength(),
                  static_cast<std::uint32_t>(10 + std::string(expected.key).size() + expected.value->size()));
        EXPECT_TRUE(has_std_header(polled_message.UserHeaders(), iggy::HeaderKind::String, expected.key,
                                   expected.value_kind, *expected.value));
    }
}

TEST_F(E2E_Message, SendMessageWithDuplicateTypedHeaderKeysThrows) {
    RecordProperty("description", "Throws when a single message contains duplicate typed header keys.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    client.CreateStream(stream_name);
    const auto stream = client.GetStream(iggy::Identifier::String(stream_name));
    TrackStream(stream.Id());
    const std::string topic_name = GetRandomName();
    iggy::TopicCreateOptions topic_options;
    topic_options.SetPartitionsCount(1)
        .SetCompressionAlgorithm(iggy::CompressionAlgorithm::None())
        .SetMessageExpiry(iggy::Expiry::NeverExpire());
    client.CreateTopic(iggy::Identifier::Numeric(stream.Id()), topic_name, topic_options);

    std::vector<iggy::HeaderEntry> headers;
    headers.push_back(iggy::HeaderEntry::Create(iggy::HeaderField::Create(iggy::HeaderKind::String, "dup-key"),
                                                iggy::HeaderField::Create(iggy::HeaderKind::String, "first")));
    headers.push_back(iggy::HeaderEntry::Create(iggy::HeaderField::Create(iggy::HeaderKind::String, "dup-key"),
                                                iggy::HeaderField::Create(iggy::HeaderKind::String, "second")));

    std::vector<iggy::IggyMessageToSend> messages;
    messages.push_back(iggy::IggyMessageToSend::Create("payload", std::move(headers)));

    ASSERT_THROW(client.SendMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0),
                                     iggy::Partitioning::PartitionId(0), messages),
                 std::exception);
}

TEST_F(E2E_Message, SendMessageWithWrongFixedWidthHeaderBytesThrows) {
    RecordProperty("description", "Throws when a typed header uses a fixed-width kind with the wrong byte count.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    client.CreateStream(stream_name);
    const auto stream = client.GetStream(iggy::Identifier::String(stream_name));
    TrackStream(stream.Id());
    const std::string topic_name = GetRandomName();
    iggy::TopicCreateOptions topic_options;
    topic_options.SetPartitionsCount(1)
        .SetCompressionAlgorithm(iggy::CompressionAlgorithm::None())
        .SetMessageExpiry(iggy::Expiry::NeverExpire());
    client.CreateTopic(iggy::Identifier::Numeric(stream.Id()), topic_name, topic_options);

    struct FixedWidthHeaderCase {
        const char *key;
        iggy::HeaderKind kind;
    };
    const FixedWidthHeaderCase fixed_width_header_cases[] = {
        {"broken-bool", iggy::HeaderKind::Bool},       {"broken-int8", iggy::HeaderKind::Int8},
        {"broken-int16", iggy::HeaderKind::Int16},     {"broken-int32", iggy::HeaderKind::Int32},
        {"broken-int64", iggy::HeaderKind::Int64},     {"broken-int128", iggy::HeaderKind::Int128},
        {"broken-uint8", iggy::HeaderKind::Uint8},     {"broken-uint16", iggy::HeaderKind::Uint16},
        {"broken-uint32", iggy::HeaderKind::Uint32},   {"broken-uint64", iggy::HeaderKind::Uint64},
        {"broken-uint128", iggy::HeaderKind::Uint128}, {"broken-float32", iggy::HeaderKind::Float32},
        {"broken-float64", iggy::HeaderKind::Float64},
    };

    for (const auto &test_case : fixed_width_header_cases) {
        SCOPED_TRACE(test_case.key);

        std::vector<iggy::HeaderEntry> headers;
        std::vector<std::uint8_t> broken_width_value;
        broken_width_value.push_back(0x01);
        broken_width_value.push_back(0x02);
        broken_width_value.push_back(0x03);
        headers.push_back(
            iggy::HeaderEntry::Create(iggy::HeaderField::Create(iggy::HeaderKind::String, test_case.key),
                                      iggy::HeaderField::Create(test_case.kind, std::move(broken_width_value))));

        std::vector<iggy::IggyMessageToSend> messages;
        messages.push_back(iggy::IggyMessageToSend::Create("payload", std::move(headers)));

        ASSERT_THROW(client.SendMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0),
                                         iggy::Partitioning::PartitionId(0), messages),
                     std::exception);
    }
}

TEST_F(E2E_Message, SendMessageWithInvalidTypedHeaderKindThrows) {
    RecordProperty("description", "Throws when a typed header uses an unsupported header kind code.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    client.CreateStream(stream_name);
    const auto stream = client.GetStream(iggy::Identifier::String(stream_name));
    TrackStream(stream.Id());
    const std::string topic_name = GetRandomName();
    iggy::TopicCreateOptions topic_options;
    topic_options.SetPartitionsCount(1)
        .SetCompressionAlgorithm(iggy::CompressionAlgorithm::None())
        .SetMessageExpiry(iggy::Expiry::NeverExpire());
    client.CreateTopic(iggy::Identifier::Numeric(stream.Id()), topic_name, topic_options);

    auto invalid_key = iggy::HeaderField::Create(static_cast<iggy::HeaderKind>(255), "bad-kind");

    std::vector<iggy::HeaderEntry> headers;
    headers.push_back(iggy::HeaderEntry::Create(std::move(invalid_key),
                                                iggy::HeaderField::Create(iggy::HeaderKind::String, "value")));

    std::vector<iggy::IggyMessageToSend> messages;
    messages.push_back(iggy::IggyMessageToSend::Create("payload", std::move(headers)));

    ASSERT_THROW(client.SendMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0),
                                     iggy::Partitioning::PartitionId(0), messages),
                 std::exception);
}

TEST_F(E2E_Message, SendMessageWithInvalidTypedHeaderSizesThrows) {
    RecordProperty("description", "Throws when typed header key or value sizes violate Rust header size constraints.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    client.CreateStream(stream_name);
    const auto stream = client.GetStream(iggy::Identifier::String(stream_name));
    TrackStream(stream.Id());
    const std::string topic_name = GetRandomName();
    iggy::TopicCreateOptions topic_options;
    topic_options.SetPartitionsCount(1)
        .SetCompressionAlgorithm(iggy::CompressionAlgorithm::None())
        .SetMessageExpiry(iggy::Expiry::NeverExpire());
    client.CreateTopic(iggy::Identifier::Numeric(stream.Id()), topic_name, topic_options);

    std::vector<iggy::HeaderEntry> empty_key_headers;
    empty_key_headers.push_back(
        iggy::HeaderEntry::Create(iggy::HeaderField::Create(iggy::HeaderKind::String, std::vector<std::uint8_t>()),
                                  iggy::HeaderField::Create(iggy::HeaderKind::String, "value")));
    std::vector<iggy::IggyMessageToSend> empty_key_messages;
    empty_key_messages.push_back(iggy::IggyMessageToSend::Create("payload", std::move(empty_key_headers)));
    ASSERT_THROW(client.SendMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0),
                                     iggy::Partitioning::PartitionId(0), empty_key_messages),
                 std::exception);

    std::vector<iggy::HeaderEntry> empty_raw_value_headers;
    empty_raw_value_headers.push_back(
        iggy::HeaderEntry::Create(iggy::HeaderField::Create(iggy::HeaderKind::String, "key"),
                                  iggy::HeaderField::Create(iggy::HeaderKind::Raw, std::vector<std::uint8_t>())));
    std::vector<iggy::IggyMessageToSend> empty_raw_value_messages;
    empty_raw_value_messages.push_back(iggy::IggyMessageToSend::Create("payload", std::move(empty_raw_value_headers)));
    ASSERT_THROW(client.SendMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0),
                                     iggy::Partitioning::PartitionId(0), empty_raw_value_messages),
                 std::exception);

    std::vector<std::uint8_t> oversized_value_bytes;
    for (std::size_t index = 0; index < 256; ++index) {
        oversized_value_bytes.push_back(index);
    }
    std::vector<iggy::HeaderEntry> oversized_value_headers;
    oversized_value_headers.push_back(
        iggy::HeaderEntry::Create(iggy::HeaderField::Create(iggy::HeaderKind::String, "key"),
                                  iggy::HeaderField::Create(iggy::HeaderKind::Raw, std::move(oversized_value_bytes))));
    std::vector<iggy::IggyMessageToSend> oversized_value_messages;
    oversized_value_messages.push_back(iggy::IggyMessageToSend::Create("payload", std::move(oversized_value_headers)));
    ASSERT_THROW(client.SendMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0),
                                     iggy::Partitioning::PartitionId(0), oversized_value_messages),
                 std::exception);

    std::vector<std::uint8_t> oversized_string_bytes;
    for (std::size_t index = 0; index < 256; ++index) {
        oversized_string_bytes.push_back('a');
    }
    std::vector<iggy::HeaderEntry> oversized_string_headers;
    oversized_string_headers.push_back(iggy::HeaderEntry::Create(
        iggy::HeaderField::Create(iggy::HeaderKind::String, "key"),
        iggy::HeaderField::Create(iggy::HeaderKind::String, std::move(oversized_string_bytes))));
    std::vector<iggy::IggyMessageToSend> oversized_string_messages;
    oversized_string_messages.push_back(
        iggy::IggyMessageToSend::Create("payload", std::move(oversized_string_headers)));
    ASSERT_THROW(client.SendMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0),
                                     iggy::Partitioning::PartitionId(0), oversized_string_messages),
                 std::exception);
}

TEST_F(E2E_Message, SendMessageAtUserHeadersSizeBoundary) {
    RecordProperty("description",
                   "Throws when encoded user headers exceed 100_000 bytes and succeeds at exactly 100_000 bytes.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    client.CreateStream(stream_name);
    const auto stream = client.GetStream(iggy::Identifier::String(stream_name));
    TrackStream(stream.Id());
    const std::string topic_name = GetRandomName();
    iggy::TopicCreateOptions topic_options;
    topic_options.SetPartitionsCount(1)
        .SetCompressionAlgorithm(iggy::CompressionAlgorithm::None())
        .SetMessageExpiry(iggy::Expiry::NeverExpire());
    client.CreateTopic(iggy::Identifier::Numeric(stream.Id()), topic_name, topic_options);

    constexpr std::uint32_t kMaxUserHeadersBytes     = 100'000u;
    constexpr std::uint32_t kFullHeaderEncodedBytes  = 267u;  // 10 bytes framing + 2-byte key + 255-byte value
    constexpr std::uint32_t kFullHeaderCount         = 374u;
    constexpr std::uint32_t kTailExactValueBytes     = 130u;
    constexpr std::uint32_t kTailOversizedValueBytes = 131u;

    std::vector<iggy::HeaderEntry> oversized_headers;
    for (std::uint32_t index = 0; index < kFullHeaderCount; ++index) {
        std::vector<std::uint8_t> key_bytes;
        key_bytes.push_back(index & 0xFF);
        key_bytes.push_back((index >> 8 & 0xFF));

        std::vector<std::uint8_t> value_bytes;
        for (std::uint32_t value_index = 0; value_index < 255u; ++value_index) {
            value_bytes.push_back(value_index);
        }

        oversized_headers.push_back(
            iggy::HeaderEntry::Create(iggy::HeaderField::Create(iggy::HeaderKind::Raw, std::move(key_bytes)),
                                      iggy::HeaderField::Create(iggy::HeaderKind::Raw, std::move(value_bytes))));
    }
    std::vector<std::uint8_t> oversized_tail_key_bytes;
    oversized_tail_key_bytes.push_back(kFullHeaderCount & 0xFF);
    oversized_tail_key_bytes.push_back((kFullHeaderCount >> 8 & 0xFF));
    std::vector<std::uint8_t> oversized_tail_value_bytes;
    for (std::uint32_t value_index = 0; value_index < kTailOversizedValueBytes; ++value_index) {
        oversized_tail_value_bytes.push_back(value_index);
    }
    oversized_headers.push_back(iggy::HeaderEntry::Create(
        iggy::HeaderField::Create(iggy::HeaderKind::Raw, std::move(oversized_tail_key_bytes)),
        iggy::HeaderField::Create(iggy::HeaderKind::Raw, std::move(oversized_tail_value_bytes))));

    std::vector<iggy::IggyMessageToSend> oversized_messages;
    oversized_messages.push_back(
        iggy::IggyMessageToSend::Create("oversized-user-headers", std::move(oversized_headers)));
    ASSERT_THROW(client.SendMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0),
                                     iggy::Partitioning::PartitionId(0), oversized_messages),
                 std::exception);

    std::vector<iggy::HeaderEntry> exact_headers;
    for (std::uint32_t index = 0; index < kFullHeaderCount; ++index) {
        std::vector<std::uint8_t> key_bytes;
        key_bytes.push_back(index & 0xFF);
        key_bytes.push_back((index >> 8 & 0xFF));

        std::vector<std::uint8_t> value_bytes;
        for (std::uint32_t value_index = 0; value_index < 255u; ++value_index) {
            value_bytes.push_back(value_index);
        }

        exact_headers.push_back(
            iggy::HeaderEntry::Create(iggy::HeaderField::Create(iggy::HeaderKind::Raw, std::move(key_bytes)),
                                      iggy::HeaderField::Create(iggy::HeaderKind::Raw, std::move(value_bytes))));
    }
    std::vector<std::uint8_t> exact_tail_key_bytes;
    exact_tail_key_bytes.push_back(kFullHeaderCount & 0xFF);
    exact_tail_key_bytes.push_back((kFullHeaderCount >> 8 & 0xFF));
    std::vector<std::uint8_t> exact_tail_value_bytes;
    for (std::uint32_t value_index = 0; value_index < kTailExactValueBytes; ++value_index) {
        exact_tail_value_bytes.push_back(value_index);
    }
    exact_headers.push_back(
        iggy::HeaderEntry::Create(iggy::HeaderField::Create(iggy::HeaderKind::Raw, std::move(exact_tail_key_bytes)),
                                  iggy::HeaderField::Create(iggy::HeaderKind::Raw, std::move(exact_tail_value_bytes))));

    std::vector<iggy::IggyMessageToSend> exact_messages;
    exact_messages.push_back(iggy::IggyMessageToSend::Create("exact-user-headers", std::move(exact_headers)));
    ASSERT_NO_THROW(client.SendMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0),
                                        iggy::Partitioning::PartitionId(0), exact_messages));

    auto polled = client.PollMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0), 0,
                                      iggy::Consumer::Single(iggy::Identifier::Numeric(1)),
                                      iggy::PollingStrategy::Offset(0), 10, false);

    ASSERT_EQ(polled.Count(), 1u);
    ASSERT_EQ(polled.Messages().size(), 1u);
    EXPECT_EQ(std::string(polled.Messages()[0].Payload().begin(), polled.Messages()[0].Payload().end()),
              "exact-user-headers");
    EXPECT_EQ(polled.Messages()[0].UserHeadersLength(), kMaxUserHeadersBytes);
    EXPECT_EQ(polled.Messages()[0].UserHeaders().size(), kFullHeaderCount + 1u);
    EXPECT_EQ(kFullHeaderCount * kFullHeaderEncodedBytes + 10u + 2u + kTailExactValueBytes, kMaxUserHeadersBytes);
}

TEST_F(E2E_Message, PollMessagesBeforeLoginThrows) {
    RecordProperty("description", "Throws when polling messages before authentication.");
    auto client = GetLoggedOutHighLevelClient();
    ASSERT_NO_THROW(client.Connect());

    ASSERT_THROW(client.PollMessages(iggy::Identifier::Numeric(1), iggy::Identifier::Numeric(0), 0,
                                     iggy::Consumer::Single(iggy::Identifier::Numeric(1)),
                                     iggy::PollingStrategy::Offset(0), 10, false),
                 std::exception);
    ASSERT_NO_THROW(client.Login("iggy", "iggy"));
    ASSERT_NO_THROW(client.Disconnect());
    ASSERT_THROW(client.PollMessages(iggy::Identifier::Numeric(1), iggy::Identifier::Numeric(0), 0,
                                     iggy::Consumer::Single(iggy::Identifier::Numeric(1)),
                                     iggy::PollingStrategy::Offset(0), 10, false),
                 std::exception);
}

TEST_F(E2E_Message, PollMessagesFromNonExistentStreamThrows) {
    RecordProperty("description", "Throws when polling messages from a non-existent stream.");
    auto client = GetLoggedInHighLevelClient();

    ASSERT_THROW(client.PollMessages(iggy::Identifier::String("nonexistent-stream-poll"), iggy::Identifier::Numeric(0),
                                     0, iggy::Consumer::Single(iggy::Identifier::Numeric(1)),
                                     iggy::PollingStrategy::Offset(0), 10, false),
                 std::exception);
}

TEST_F(E2E_Message, PollMessagesCountLessThanAvailable) {
    RecordProperty("description", "Returns only the requested count when fewer messages are requested than available.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    client.CreateStream(stream_name);
    const auto stream = client.GetStream(iggy::Identifier::String(stream_name));
    TrackStream(stream.Id());
    const std::string topic_name = GetRandomName();
    iggy::TopicCreateOptions topic_options;
    topic_options.SetPartitionsCount(1)
        .SetCompressionAlgorithm(iggy::CompressionAlgorithm::None())
        .SetMessageExpiry(iggy::Expiry::NeverExpire());
    client.CreateTopic(iggy::Identifier::Numeric(stream.Id()), topic_name, topic_options);

    std::vector<iggy::IggyMessageToSend> messages;
    for (std::uint32_t i = 0; i < 10; i++) {
        auto msg = iggy::IggyMessageToSend::Create("msg-" + std::to_string(i), std::vector<iggy::HeaderEntry>());
        messages.push_back(std::move(msg));
    }

    client.SendMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0),
                        iggy::Partitioning::PartitionId(0), messages);

    auto polled = client.PollMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0), 0,
                                      iggy::Consumer::Single(iggy::Identifier::Numeric(1)),
                                      iggy::PollingStrategy::Offset(0), 5, false);

    ASSERT_EQ(polled.Count(), 5u);
    ASSERT_EQ(polled.Messages().size(), 5u);
}

TEST_F(E2E_Message, PollMessagesWithLargeOffset) {
    RecordProperty("description", "Returns zero messages when polling with an offset beyond available messages.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    client.CreateStream(stream_name);
    const auto stream = client.GetStream(iggy::Identifier::String(stream_name));
    TrackStream(stream.Id());
    const std::string topic_name = GetRandomName();
    iggy::TopicCreateOptions topic_options;
    topic_options.SetPartitionsCount(1)
        .SetCompressionAlgorithm(iggy::CompressionAlgorithm::None())
        .SetMessageExpiry(iggy::Expiry::NeverExpire());
    client.CreateTopic(iggy::Identifier::Numeric(stream.Id()), topic_name, topic_options);

    std::vector<iggy::IggyMessageToSend> messages;
    for (std::uint32_t i = 0; i < 5; i++) {
        auto msg = iggy::IggyMessageToSend::Create("msg-" + std::to_string(i), std::vector<iggy::HeaderEntry>());
        messages.push_back(std::move(msg));
    }

    client.SendMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0),
                        iggy::Partitioning::PartitionId(0), messages);

    auto polled = client.PollMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0), 0,
                                      iggy::Consumer::Single(iggy::Identifier::Numeric(1)),
                                      iggy::PollingStrategy::Offset(999999), 100, false);

    ASSERT_EQ(polled.Count(), 0u);
    ASSERT_EQ(polled.Messages().size(), 0u);
}

TEST_F(E2E_Message, PollMessagesFirstStrategy) {
    RecordProperty("description", "Verifies first polling strategy returns messages from the beginning.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    client.CreateStream(stream_name);
    const auto stream = client.GetStream(iggy::Identifier::String(stream_name));
    TrackStream(stream.Id());
    const std::string topic_name = GetRandomName();
    iggy::TopicCreateOptions topic_options;
    topic_options.SetPartitionsCount(1)
        .SetCompressionAlgorithm(iggy::CompressionAlgorithm::None())
        .SetMessageExpiry(iggy::Expiry::NeverExpire());
    client.CreateTopic(iggy::Identifier::Numeric(stream.Id()), topic_name, topic_options);

    std::vector<iggy::IggyMessageToSend> messages;
    for (std::uint32_t i = 0; i < 10; i++) {
        auto msg = iggy::IggyMessageToSend::Create("msg-" + std::to_string(i), std::vector<iggy::HeaderEntry>());
        messages.push_back(std::move(msg));
    }

    client.SendMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0),
                        iggy::Partitioning::PartitionId(0), messages);

    auto polled = client.PollMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0), 0,
                                      iggy::Consumer::Single(iggy::Identifier::Numeric(1)),
                                      iggy::PollingStrategy::First(), 3, false);

    ASSERT_EQ(polled.Count(), 3u);
    ASSERT_EQ(polled.Messages().size(), 3u);
    EXPECT_EQ(polled.Messages()[0].Offset(), 0u);
    for (std::uint32_t i = 0; i < 3; i++) {
        EXPECT_EQ(polled.Messages()[i].Offset(), static_cast<std::uint64_t>(i));
        std::string expected = "msg-" + std::to_string(i);
        std::string actual(polled.Messages()[i].Payload().begin(), polled.Messages()[i].Payload().end());
        EXPECT_EQ(actual, expected) << "Payload mismatch at offset " << i;
    }
}

TEST_F(E2E_Message, PollMessagesLastStrategy) {
    RecordProperty("description", "Verifies last polling strategy returns messages from the end.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    client.CreateStream(stream_name);
    const auto stream = client.GetStream(iggy::Identifier::String(stream_name));
    TrackStream(stream.Id());
    const std::string topic_name = GetRandomName();
    iggy::TopicCreateOptions topic_options;
    topic_options.SetPartitionsCount(1)
        .SetCompressionAlgorithm(iggy::CompressionAlgorithm::None())
        .SetMessageExpiry(iggy::Expiry::NeverExpire());
    client.CreateTopic(iggy::Identifier::Numeric(stream.Id()), topic_name, topic_options);

    std::vector<iggy::IggyMessageToSend> messages;
    for (std::uint32_t i = 0; i < 10; i++) {
        auto msg = iggy::IggyMessageToSend::Create("msg-" + std::to_string(i), std::vector<iggy::HeaderEntry>());
        messages.push_back(std::move(msg));
    }

    client.SendMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0),
                        iggy::Partitioning::PartitionId(0), messages);

    auto polled = client.PollMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0), 0,
                                      iggy::Consumer::Single(iggy::Identifier::Numeric(1)),
                                      iggy::PollingStrategy::Last(), 3, false);

    ASSERT_EQ(polled.Count(), 3u);
    ASSERT_EQ(polled.Messages().size(), 3u);
    EXPECT_EQ(polled.Messages()[0].Offset(), 7u);
    EXPECT_EQ(polled.Messages()[2].Offset(), 9u);
    for (std::uint32_t i = 0; i < 3; i++) {
        std::string expected = "msg-" + std::to_string(7 + i);
        std::string actual(polled.Messages()[i].Payload().begin(), polled.Messages()[i].Payload().end());
        EXPECT_EQ(actual, expected) << "Payload mismatch at index " << i;
    }
}

TEST_F(E2E_Message, PollMessagesNextStrategyNoAutoCommit) {
    RecordProperty("description",
                   "Verifies next strategy without auto-commit returns the same messages on repeated calls.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    client.CreateStream(stream_name);
    const auto stream = client.GetStream(iggy::Identifier::String(stream_name));
    TrackStream(stream.Id());
    const std::string topic_name = GetRandomName();
    iggy::TopicCreateOptions topic_options;
    topic_options.SetPartitionsCount(1)
        .SetCompressionAlgorithm(iggy::CompressionAlgorithm::None())
        .SetMessageExpiry(iggy::Expiry::NeverExpire());
    client.CreateTopic(iggy::Identifier::Numeric(stream.Id()), topic_name, topic_options);

    std::vector<iggy::IggyMessageToSend> messages;
    for (std::uint32_t i = 0; i < 5; i++) {
        auto msg = iggy::IggyMessageToSend::Create("msg-" + std::to_string(i), std::vector<iggy::HeaderEntry>());
        messages.push_back(std::move(msg));
    }

    client.SendMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0),
                        iggy::Partitioning::PartitionId(0), messages);

    auto polled1 = client.PollMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0), 0,
                                       iggy::Consumer::Single(iggy::Identifier::Numeric(1)),
                                       iggy::PollingStrategy::Next(), 100, false);
    ASSERT_EQ(polled1.Count(), 5u);

    auto polled2 = client.PollMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0), 0,
                                       iggy::Consumer::Single(iggy::Identifier::Numeric(1)),
                                       iggy::PollingStrategy::Next(), 100, false);
    ASSERT_EQ(polled2.Count(), 5u);
    for (std::uint32_t i = 0; i < 5; i++) {
        EXPECT_EQ(polled1.Messages()[i].Offset(), static_cast<std::uint64_t>(i));
        std::string expected = "msg-" + std::to_string(i);
        std::string actual(polled1.Messages()[i].Payload().begin(), polled1.Messages()[i].Payload().end());
        EXPECT_EQ(actual, expected) << "polled1 payload mismatch at index " << i;
    }
    for (std::uint32_t i = 0; i < 5; i++) {
        EXPECT_EQ(polled2.Messages()[i].Offset(), static_cast<std::uint64_t>(i));
        std::string expected = "msg-" + std::to_string(i);
        std::string actual(polled2.Messages()[i].Payload().begin(), polled2.Messages()[i].Payload().end());
        EXPECT_EQ(actual, expected) << "polled2 payload mismatch at index " << i;
    }
}

TEST_F(E2E_Message, PollMessagesNextStrategyAutoCommit) {
    RecordProperty("description", "Verifies next strategy with auto-commit advances the offset on subsequent polls.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    client.CreateStream(stream_name);
    const auto stream = client.GetStream(iggy::Identifier::String(stream_name));
    TrackStream(stream.Id());
    const std::string topic_name = GetRandomName();
    iggy::TopicCreateOptions topic_options;
    topic_options.SetPartitionsCount(1)
        .SetCompressionAlgorithm(iggy::CompressionAlgorithm::None())
        .SetMessageExpiry(iggy::Expiry::NeverExpire());
    client.CreateTopic(iggy::Identifier::Numeric(stream.Id()), topic_name, topic_options);

    std::vector<iggy::IggyMessageToSend> messages;
    for (std::uint32_t i = 0; i < 10; i++) {
        auto msg = iggy::IggyMessageToSend::Create("msg-" + std::to_string(i), std::vector<iggy::HeaderEntry>());
        messages.push_back(std::move(msg));
    }

    client.SendMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0),
                        iggy::Partitioning::PartitionId(0), messages);

    auto polled1 = client.PollMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0), 0,
                                       iggy::Consumer::Single(iggy::Identifier::Numeric(1)),
                                       iggy::PollingStrategy::Next(), 5, true);
    ASSERT_EQ(polled1.Count(), 5u);
    EXPECT_EQ(polled1.Messages()[0].Offset(), 0u);
    EXPECT_EQ(polled1.Messages()[4].Offset(), 4u);

    auto polled2 = client.PollMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0), 0,
                                       iggy::Consumer::Single(iggy::Identifier::Numeric(1)),
                                       iggy::PollingStrategy::Next(), 5, true);
    ASSERT_EQ(polled2.Count(), 5u);
    EXPECT_EQ(polled2.Messages()[0].Offset(), 5u);
    EXPECT_EQ(polled2.Messages()[4].Offset(), 9u);
    for (std::uint32_t i = 0; i < 5; i++) {
        std::string expected1 = "msg-" + std::to_string(i);
        std::string actual1(polled1.Messages()[i].Payload().begin(), polled1.Messages()[i].Payload().end());
        EXPECT_EQ(actual1, expected1) << "polled1 payload mismatch at index " << i;
    }
    for (std::uint32_t i = 0; i < 5; i++) {
        std::string expected2 = "msg-" + std::to_string(5 + i);
        std::string actual2(polled2.Messages()[i].Payload().begin(), polled2.Messages()[i].Payload().end());
        EXPECT_EQ(actual2, expected2) << "polled2 payload mismatch at index " << i;
    }

    auto polled3 = client.PollMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0), 0,
                                       iggy::Consumer::Single(iggy::Identifier::Numeric(1)),
                                       iggy::PollingStrategy::Next(), 5, true);
    ASSERT_EQ(polled3.Count(), 0u);
}

TEST_F(E2E_Message, PollMessagesConsumerIdIndependence) {
    RecordProperty("description", "Verifies different consumer IDs maintain independent offsets.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    client.CreateStream(stream_name);
    const auto stream = client.GetStream(iggy::Identifier::String(stream_name));
    TrackStream(stream.Id());
    const std::string topic_name = GetRandomName();
    iggy::TopicCreateOptions topic_options;
    topic_options.SetPartitionsCount(1)
        .SetCompressionAlgorithm(iggy::CompressionAlgorithm::None())
        .SetMessageExpiry(iggy::Expiry::NeverExpire());
    client.CreateTopic(iggy::Identifier::Numeric(stream.Id()), topic_name, topic_options);

    std::vector<iggy::IggyMessageToSend> messages;
    for (std::uint32_t i = 0; i < 5; i++) {
        auto msg = iggy::IggyMessageToSend::Create("msg-" + std::to_string(i), std::vector<iggy::HeaderEntry>());
        messages.push_back(std::move(msg));
    }

    client.SendMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0),
                        iggy::Partitioning::PartitionId(0), messages);

    auto polled_c1 = client.PollMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0), 0,
                                         iggy::Consumer::Single(iggy::Identifier::Numeric(1)),
                                         iggy::PollingStrategy::Next(), 3, true);
    ASSERT_EQ(polled_c1.Count(), 3u);

    auto polled_c2 = client.PollMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0), 0,
                                         iggy::Consumer::Single(iggy::Identifier::Numeric(2)),
                                         iggy::PollingStrategy::Next(), 5, true);
    ASSERT_EQ(polled_c2.Count(), 5u);

    auto polled_c1_again = client.PollMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0), 0,
                                               iggy::Consumer::Single(iggy::Identifier::Numeric(1)),
                                               iggy::PollingStrategy::Next(), 5, true);
    ASSERT_EQ(polled_c1_again.Count(), 2u);
}

TEST_F(E2E_Message, PollMessagesMultipleSendsThenPollOrder) {
    RecordProperty("description", "Verifies message ordering is preserved across multiple send batches.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    client.CreateStream(stream_name);
    const auto stream = client.GetStream(iggy::Identifier::String(stream_name));
    TrackStream(stream.Id());
    const std::string topic_name = GetRandomName();
    iggy::TopicCreateOptions topic_options;
    topic_options.SetPartitionsCount(1)
        .SetCompressionAlgorithm(iggy::CompressionAlgorithm::None())
        .SetMessageExpiry(iggy::Expiry::NeverExpire());
    client.CreateTopic(iggy::Identifier::Numeric(stream.Id()), topic_name, topic_options);

    std::vector<iggy::IggyMessageToSend> batch1;
    for (std::uint32_t i = 0; i < 5; i++) {
        auto msg = iggy::IggyMessageToSend::Create("batch1-" + std::to_string(i), std::vector<iggy::HeaderEntry>());
        batch1.push_back(std::move(msg));
    }
    client.SendMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0),
                        iggy::Partitioning::PartitionId(0), batch1);

    std::vector<iggy::IggyMessageToSend> batch2;
    for (std::uint32_t i = 0; i < 5; i++) {
        auto msg = iggy::IggyMessageToSend::Create("batch2-" + std::to_string(i), std::vector<iggy::HeaderEntry>());
        batch2.push_back(std::move(msg));
    }
    client.SendMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0),
                        iggy::Partitioning::PartitionId(0), batch2);

    auto polled = client.PollMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0), 0,
                                      iggy::Consumer::Single(iggy::Identifier::Numeric(1)),
                                      iggy::PollingStrategy::Offset(0), 100, false);

    ASSERT_EQ(polled.Count(), 10u);
    for (std::uint32_t i = 0; i < 10; i++) {
        EXPECT_EQ(polled.Messages()[i].Offset(), static_cast<std::uint64_t>(i)) << "Offset mismatch at index " << i;
    }
    for (std::uint32_t i = 0; i < 5; i++) {
        std::string expected = "batch1-" + std::to_string(i);
        std::string actual(polled.Messages()[i].Payload().begin(), polled.Messages()[i].Payload().end());
        EXPECT_EQ(actual, expected) << "batch1 payload mismatch at index " << i;
    }
    for (std::uint32_t i = 0; i < 5; i++) {
        std::string expected = "batch2-" + std::to_string(i);
        std::string actual(polled.Messages()[5 + i].Payload().begin(), polled.Messages()[5 + i].Payload().end());
        EXPECT_EQ(actual, expected) << "batch2 payload mismatch at index " << i;
    }
}

TEST_F(E2E_Message, PollMessagesMultipleCustomIds) {
    RecordProperty("description", "Verifies multiple messages with distinct custom IDs are all preserved.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    client.CreateStream(stream_name);
    const auto stream = client.GetStream(iggy::Identifier::String(stream_name));
    TrackStream(stream.Id());
    const std::string topic_name = GetRandomName();
    iggy::TopicCreateOptions topic_options;
    topic_options.SetPartitionsCount(1)
        .SetCompressionAlgorithm(iggy::CompressionAlgorithm::None())
        .SetMessageExpiry(iggy::Expiry::NeverExpire());
    client.CreateTopic(iggy::Identifier::Numeric(stream.Id()), topic_name, topic_options);

    const std::uint64_t id_values[] = {100, 200, 300, 400, 500};
    std::vector<iggy::IggyMessageToSend> messages;
    for (std::uint32_t i = 0; i < 5; i++) {
        messages.push_back(iggy::IggyMessageToSend::Create("msg-" + std::to_string(i), std::vector<iggy::HeaderEntry>(),
                                                           static_cast<absl::uint128>(id_values[i])));
    }

    client.SendMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0),
                        iggy::Partitioning::PartitionId(0), messages);

    auto polled = client.PollMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0), 0,
                                      iggy::Consumer::Single(iggy::Identifier::Numeric(1)),
                                      iggy::PollingStrategy::Offset(0), 100, false);

    ASSERT_EQ(polled.Count(), 5u);
    for (std::uint32_t i = 0; i < 5; i++) {
        EXPECT_EQ(polled.Messages()[i].Id(), static_cast<absl::uint128>(id_values[i])) << "ID mismatch at index " << i;
    }
}

TEST_F(E2E_Message, PollMessagesAfterStreamDeletedThrows) {
    RecordProperty("description", "Throws when polling messages after the stream has been deleted.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    client.CreateStream(stream_name);
    const auto stream = client.GetStream(iggy::Identifier::String(stream_name));
    TrackStream(stream.Id());
    const std::string topic_name = GetRandomName();
    iggy::TopicCreateOptions topic_options;
    topic_options.SetPartitionsCount(1)
        .SetCompressionAlgorithm(iggy::CompressionAlgorithm::None())
        .SetMessageExpiry(iggy::Expiry::NeverExpire());
    client.CreateTopic(iggy::Identifier::Numeric(stream.Id()), topic_name, topic_options);

    std::vector<iggy::IggyMessageToSend> messages;
    messages.push_back(iggy::IggyMessageToSend::Create("test", {}));
    client.SendMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0),
                        iggy::Partitioning::PartitionId(0), messages);

    const std::uint32_t saved_stream_id = stream.Id();
    client.DeleteStream(iggy::Identifier::Numeric(saved_stream_id));
    ForgetTrackedStream(saved_stream_id);

    ASSERT_THROW(client.PollMessages(iggy::Identifier::Numeric(saved_stream_id), iggy::Identifier::Numeric(0), 0,
                                     iggy::Consumer::Single(iggy::Identifier::Numeric(1)),
                                     iggy::PollingStrategy::Offset(0), 10, false),
                 std::exception);
}

TEST_F(E2E_Message, PollMessagesWithInvalidPartitionIdThrows) {
    RecordProperty("description", "Throws when polling with a non-existent partition ID.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    client.CreateStream(stream_name);
    const auto stream = client.GetStream(iggy::Identifier::String(stream_name));
    TrackStream(stream.Id());
    const std::string topic_name = GetRandomName();
    iggy::TopicCreateOptions topic_options;
    topic_options.SetPartitionsCount(1)
        .SetCompressionAlgorithm(iggy::CompressionAlgorithm::None())
        .SetMessageExpiry(iggy::Expiry::NeverExpire());
    client.CreateTopic(iggy::Identifier::Numeric(stream.Id()), topic_name, topic_options);

    ASSERT_THROW(client.PollMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0), 9999,
                                     iggy::Consumer::Single(iggy::Identifier::Numeric(1)),
                                     iggy::PollingStrategy::Offset(0), 10, false),
                 std::exception);
}

TEST_F(E2E_Message, PollMessagesWithCountZeroThrows) {
    RecordProperty("description", "Throws when polling with count=0.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    client.CreateStream(stream_name);
    const auto stream = client.GetStream(iggy::Identifier::String(stream_name));
    TrackStream(stream.Id());
    const std::string topic_name = GetRandomName();
    iggy::TopicCreateOptions topic_options;
    topic_options.SetPartitionsCount(1)
        .SetCompressionAlgorithm(iggy::CompressionAlgorithm::None())
        .SetMessageExpiry(iggy::Expiry::NeverExpire());
    client.CreateTopic(iggy::Identifier::Numeric(stream.Id()), topic_name, topic_options);

    ASSERT_THROW(client.PollMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0), 0,
                                     iggy::Consumer::Single(iggy::Identifier::Numeric(1)),
                                     iggy::PollingStrategy::Offset(0), 0, false),
                 std::exception);
}

TEST_F(E2E_Message, PollMessagesWithoutSpecifyingPartition) {
    RecordProperty("description",
                   "Verifies polling with an omitted partition defaults to partition 0 and returns messages.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    client.CreateStream(stream_name);
    const auto stream = client.GetStream(iggy::Identifier::String(stream_name));
    TrackStream(stream.Id());
    const std::string topic_name = GetRandomName();
    iggy::TopicCreateOptions topic_options;
    topic_options.SetPartitionsCount(1)
        .SetCompressionAlgorithm(iggy::CompressionAlgorithm::None())
        .SetMessageExpiry(iggy::Expiry::NeverExpire());
    client.CreateTopic(iggy::Identifier::Numeric(stream.Id()), topic_name, topic_options);

    std::vector<iggy::IggyMessageToSend> messages;
    for (std::uint32_t i = 0; i < 5; i++) {
        auto msg = iggy::IggyMessageToSend::Create("msg-" + std::to_string(i), std::vector<iggy::HeaderEntry>());
        messages.push_back(std::move(msg));
    }
    client.SendMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0),
                        iggy::Partitioning::PartitionId(0), messages);

    auto polled = client.PollMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0),
                                      std::nullopt, iggy::Consumer::Single(iggy::Identifier::Numeric(1)),
                                      iggy::PollingStrategy::Offset(0), 100, false);

    // An omitted partition lets the server pick one. With a single
    // partition topic that should always be partition 0.
    ASSERT_EQ(polled.PartitionId(), 0u) << "Omitted partition did not resolve to partition 0";
    ASSERT_EQ(polled.Count(), 5u);
    ASSERT_EQ(polled.Messages().size(), 5u);
    for (std::uint32_t i = 0; i < 5; i++) {
        std::string expected = "msg-" + std::to_string(i);
        std::string actual(polled.Messages()[i].Payload().begin(), polled.Messages()[i].Payload().end());
        EXPECT_EQ(actual, expected) << "Payload mismatch at index " << i;
    }
}

TEST_F(E2E_Message, PollMessagesTimestampStrategy) {
    RecordProperty("description",
                   "Verifies timestamp polling strategy returns messages with timestamp >= the specified value.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    client.CreateStream(stream_name);
    const auto stream = client.GetStream(iggy::Identifier::String(stream_name));
    TrackStream(stream.Id());
    const std::string topic_name = GetRandomName();
    iggy::TopicCreateOptions topic_options;
    topic_options.SetPartitionsCount(1)
        .SetCompressionAlgorithm(iggy::CompressionAlgorithm::None())
        .SetMessageExpiry(iggy::Expiry::NeverExpire());
    client.CreateTopic(iggy::Identifier::Numeric(stream.Id()), topic_name, topic_options);

    std::vector<iggy::IggyMessageToSend> batch1;
    for (std::uint32_t i = 0; i < 5; i++) {
        auto msg = iggy::IggyMessageToSend::Create("batch1-" + std::to_string(i), std::vector<iggy::HeaderEntry>());
        batch1.push_back(std::move(msg));
    }
    client.SendMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0),
                        iggy::Partitioning::PartitionId(0), batch1);

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    std::vector<iggy::IggyMessageToSend> batch2;
    for (std::uint32_t i = 0; i < 5; i++) {
        auto msg = iggy::IggyMessageToSend::Create("batch2-" + std::to_string(i), std::vector<iggy::HeaderEntry>());
        batch2.push_back(std::move(msg));
    }
    client.SendMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0),
                        iggy::Partitioning::PartitionId(0), batch2);

    auto all = client.PollMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0), 0,
                                   iggy::Consumer::Single(iggy::Identifier::Numeric(1)),
                                   iggy::PollingStrategy::Offset(0), 100, false);
    ASSERT_EQ(all.Count(), 10u);

    // IggyTimestamp::now() is microsecond-resolution and we slept 100ms between batches; a gap
    // smaller than half that window means the test has degraded into a tautology on busy CI.
    constexpr std::uint64_t kMinTimestampGapMicros = 50'000;
    std::uint64_t batch1_timestamp                 = all.Messages()[0].Timestamp();
    std::uint64_t batch2_timestamp                 = all.Messages()[5].Timestamp();
    ASSERT_GT(batch2_timestamp, batch1_timestamp);
    ASSERT_GE(batch2_timestamp - batch1_timestamp, kMinTimestampGapMicros)
        << "Timestamp gap collapsed (" << (batch2_timestamp - batch1_timestamp)
        << "us) — test no longer exercises timestamp filtering";

    auto polled = client.PollMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0), 0,
                                      iggy::Consumer::Single(iggy::Identifier::Numeric(2)),
                                      iggy::PollingStrategy::Timestamp(batch2_timestamp), 100, false);

    ASSERT_GE(polled.Count(), 5u);
    // The server contract is `timestamp >= polling_strategy_value`. If a batch1 message lands on
    // exactly the same microsecond as batch2's first message, the count can legitimately exceed 5,
    // so verify by prefix rather than indexing each message against `batch2-N`.
    for (std::size_t i = 0; i < polled.Messages().size(); i++) {
        EXPECT_GE(polled.Messages()[i].Timestamp(), batch2_timestamp)
            << "Message at index " << i << " has earlier timestamp";
        std::string actual(polled.Messages()[i].Payload().begin(), polled.Messages()[i].Payload().end());
        EXPECT_TRUE(actual.rfind("batch1-", 0) == 0 || actual.rfind("batch2-", 0) == 0)
            << "Polled message at index " << i << " has unexpected payload: " << actual;
    }
}

TEST_F(E2E_Message, PollMessagesMonotonicOffsets) {
    RecordProperty("description",
                   "Verifies offsets are monotonically increasing and continuous across multiple polls.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    client.CreateStream(stream_name);
    const auto stream = client.GetStream(iggy::Identifier::String(stream_name));
    TrackStream(stream.Id());
    const std::string topic_name = GetRandomName();
    iggy::TopicCreateOptions topic_options;
    topic_options.SetPartitionsCount(1)
        .SetCompressionAlgorithm(iggy::CompressionAlgorithm::None())
        .SetMessageExpiry(iggy::Expiry::NeverExpire());
    client.CreateTopic(iggy::Identifier::Numeric(stream.Id()), topic_name, topic_options);

    std::vector<iggy::IggyMessageToSend> messages;
    for (std::uint32_t i = 0; i < 20; i++) {
        auto msg = iggy::IggyMessageToSend::Create("mono-" + std::to_string(i), std::vector<iggy::HeaderEntry>());
        messages.push_back(std::move(msg));
    }
    client.SendMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0),
                        iggy::Partitioning::PartitionId(0), messages);

    std::uint64_t expected_offset = 0;
    for (int chunk = 0; chunk < 4; chunk++) {
        auto polled = client.PollMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0), 0,
                                          iggy::Consumer::Single(iggy::Identifier::Numeric(1)),
                                          iggy::PollingStrategy::Offset(expected_offset), 5, false);

        ASSERT_EQ(polled.Count(), 5u) << "Chunk " << chunk;
        ASSERT_EQ(polled.Messages().size(), 5u) << "Chunk " << chunk;

        for (std::size_t i = 0; i < polled.Messages().size(); i++) {
            EXPECT_EQ(polled.Messages()[i].Offset(), expected_offset) << "Chunk " << chunk << " index " << i;
            expected_offset++;
        }
    }

    ASSERT_EQ(expected_offset, 20u);
}

TEST_F(E2E_Message, SendMessagesLargeBatch) {
    RecordProperty("description", "Verifies sending a large batch of 1000 messages succeeds and all are retrievable.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    client.CreateStream(stream_name);
    const auto stream = client.GetStream(iggy::Identifier::String(stream_name));
    TrackStream(stream.Id());
    const std::string topic_name = GetRandomName();
    iggy::TopicCreateOptions topic_options;
    topic_options.SetPartitionsCount(1)
        .SetCompressionAlgorithm(iggy::CompressionAlgorithm::None())
        .SetMessageExpiry(iggy::Expiry::NeverExpire());
    client.CreateTopic(iggy::Identifier::Numeric(stream.Id()), topic_name, topic_options);

    std::vector<iggy::IggyMessageToSend> messages;
    for (std::uint32_t i = 0; i < 1000; i++) {
        auto msg = iggy::IggyMessageToSend::Create("batch-msg-" + std::to_string(i), std::vector<iggy::HeaderEntry>());
        messages.push_back(std::move(msg));
    }

    ASSERT_NO_THROW(client.SendMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0),
                                        iggy::Partitioning::PartitionId(0), messages));

    auto polled = client.PollMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0), 0,
                                      iggy::Consumer::Single(iggy::Identifier::Numeric(1)),
                                      iggy::PollingStrategy::Offset(0), 1000, false);

    ASSERT_EQ(polled.Count(), 1000u);
    ASSERT_EQ(polled.Messages().size(), 1000u);
    EXPECT_EQ(polled.Messages()[0].Offset(), 0u);
    EXPECT_EQ(polled.Messages()[999].Offset(), 999u);
}

TEST_F(E2E_Message, ConsumerGroupCreateJoinAndPollMessages) {
    RecordProperty("description",
                   "Creates a consumer group, joins it, sends messages, and polls them using consumer_group kind.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    client.CreateStream(stream_name);
    const auto stream = client.GetStream(iggy::Identifier::String(stream_name));
    TrackStream(stream.Id());
    const std::string topic_name = GetRandomName();
    iggy::TopicCreateOptions topic_options;
    topic_options.SetPartitionsCount(1)
        .SetCompressionAlgorithm(iggy::CompressionAlgorithm::None())
        .SetMessageExpiry(iggy::Expiry::NeverExpire());
    client.CreateTopic(iggy::Identifier::Numeric(stream.Id()), topic_name, topic_options);

    const std::string group_name = GetRandomName();
    auto group =
        client.CreateConsumerGroup(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0), group_name);
    ASSERT_EQ(group.MembersCount(), 0u);

    ASSERT_NO_THROW(client.JoinConsumerGroup(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0),
                                             iggy::Identifier::Numeric(group.Id())));

    const auto group_after_join = client.GetConsumerGroup(
        iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0), iggy::Identifier::Numeric(group.Id()));
    ASSERT_EQ(group_after_join.MembersCount(), 1u);

    std::vector<iggy::IggyMessageToSend> messages;
    for (std::uint32_t i = 0; i < 10; i++) {
        auto msg = iggy::IggyMessageToSend::Create("cg-msg-" + std::to_string(i), std::vector<iggy::HeaderEntry>());
        messages.push_back(std::move(msg));
    }
    client.SendMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0),
                        iggy::Partitioning::PartitionId(0), messages);

    auto polled = client.PollMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0), 0,
                                      iggy::Consumer::Group(iggy::Identifier::Numeric(group.Id())),
                                      iggy::PollingStrategy::Offset(0), 100, false);

    ASSERT_EQ(polled.Count(), 10u);
    ASSERT_EQ(polled.Messages().size(), 10u);
    for (std::uint32_t i = 0; i < 10; i++) {
        std::string expected = "cg-msg-" + std::to_string(i);
        std::string actual(polled.Messages()[i].Payload().begin(), polled.Messages()[i].Payload().end());
        EXPECT_EQ(actual, expected) << "Payload mismatch at offset " << i;
    }

    ASSERT_NO_THROW(client.LeaveConsumerGroup(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0),
                                              iggy::Identifier::Numeric(group.Id())));

    const auto group_after_leave = client.GetConsumerGroup(
        iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0), iggy::Identifier::Numeric(group.Id()));
    ASSERT_EQ(group_after_leave.MembersCount(), 0u);
}

TEST_F(E2E_Message, PollMessagesWithDistinctConsumersKeepsOffsetsIndependent) {
    RecordProperty("description", "Each named consumer owns its offset, so both read the whole partition.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    client.CreateStream(stream_name);
    const auto stream = client.GetStream(iggy::Identifier::String(stream_name));
    TrackStream(stream.Id());
    const std::string topic_name = GetRandomName();
    client.CreateTopic(iggy::Identifier::Numeric(stream.Id()), topic_name,
                       iggy::TopicCreateOptions().SetPartitionsCount(1));

    std::vector<iggy::IggyMessageToSend> messages;
    for (std::uint32_t i = 0; i < 3; i++) {
        messages.push_back(
            iggy::IggyMessageToSend::Create("isolated-" + std::to_string(i), std::vector<iggy::HeaderEntry>()));
    }
    client.SendMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0),
                        iggy::Partitioning::PartitionId(0), messages);

    for (const std::string &consumer_name :
         {std::string("isolation-consumer-a"), std::string("isolation-consumer-b")}) {
        auto polled = client.PollMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0), 0,
                                          iggy::Consumer::Single(iggy::Identifier::String(consumer_name)),
                                          iggy::PollingStrategy::Next(), 10, true);

        ASSERT_EQ(polled.Count(), 3u) << "Consumer " << consumer_name << " did not read the whole partition";
        for (std::uint32_t i = 0; i < 3; i++) {
            std::string expected = "isolated-" + std::to_string(i);
            std::string actual(polled.Messages()[i].Payload().begin(), polled.Messages()[i].Payload().end());
            EXPECT_EQ(actual, expected) << "Payload mismatch at offset " << i;
        }
    }
}

TEST_F(E2E_Message, PollMessagesWithSharedConsumerSplitsThePartition) {
    RecordProperty("description", "Two polls under one consumer name share a stored offset, so the second reads none.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    client.CreateStream(stream_name);
    const auto stream = client.GetStream(iggy::Identifier::String(stream_name));
    TrackStream(stream.Id());
    const std::string topic_name = GetRandomName();
    client.CreateTopic(iggy::Identifier::Numeric(stream.Id()), topic_name,
                       iggy::TopicCreateOptions().SetPartitionsCount(1));

    std::vector<iggy::IggyMessageToSend> messages;
    for (std::uint32_t i = 0; i < 3; i++) {
        messages.push_back(
            iggy::IggyMessageToSend::Create("shared-" + std::to_string(i), std::vector<iggy::HeaderEntry>()));
    }
    client.SendMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0),
                        iggy::Partitioning::PartitionId(0), messages);

    const auto consumer = iggy::Consumer::Single(iggy::Identifier::String("shared-consumer"));
    auto first_poll     = client.PollMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0), 0,
                                              consumer, iggy::PollingStrategy::Next(), 10, true);
    ASSERT_EQ(first_poll.Count(), 3u);

    auto second_poll = client.PollMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0), 0,
                                           consumer, iggy::PollingStrategy::Next(), 10, true);
    ASSERT_EQ(second_poll.Count(), 0u);
}

TEST_F(E2E_Message, PollMessagesWithConsumerGroupReadsAssignedPartitions) {
    RecordProperty(
        "description",
        "A group member polling without a partition reads its assigned partitions, one per call, rather than "
        "falling back to partition 0.");
    const std::string stream_name = GetRandomName();
    auto client                   = GetLoggedInHighLevelClient();

    client.CreateStream(stream_name);
    const auto stream = client.GetStream(iggy::Identifier::String(stream_name));
    TrackStream(stream.Id());
    const std::string topic_name = GetRandomName();
    client.CreateTopic(iggy::Identifier::Numeric(stream.Id()), topic_name,
                       iggy::TopicCreateOptions().SetPartitionsCount(2));

    const std::string group_name = GetRandomName();
    client.CreateConsumerGroup(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0), group_name);
    TrackConsumerGroup(stream_name, topic_name, group_name);
    client.JoinConsumerGroup(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0),
                             iggy::Identifier::String(group_name));

    for (std::uint32_t partition_id = 0; partition_id < 2; partition_id++) {
        std::vector<iggy::IggyMessageToSend> messages;
        for (std::uint32_t i = 0; i < 2; i++) {
            messages.push_back(iggy::IggyMessageToSend::Create(
                "p" + std::to_string(partition_id) + "-" + std::to_string(i), std::vector<iggy::HeaderEntry>()));
        }
        client.SendMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0),
                            iggy::Partitioning::PartitionId(partition_id), messages);
    }

    // Each poll takes the next partition the member owns, so two polls cover
    // both. A client falling back to partition 0 would read it twice instead.
    std::unordered_set<std::uint32_t> polled_partitions;
    std::unordered_set<std::string> polled_payloads;
    for (std::uint32_t poll = 0; poll < 2; poll++) {
        auto polled = client.PollMessages(iggy::Identifier::Numeric(stream.Id()), iggy::Identifier::Numeric(0),
                                          std::nullopt, iggy::Consumer::Group(iggy::Identifier::String(group_name)),
                                          iggy::PollingStrategy::Next(), 10, true);

        ASSERT_EQ(polled.Count(), 2u) << "Poll " << poll << " did not read a whole partition";
        polled_partitions.insert(polled.PartitionId());
        for (const auto &message : polled.Messages()) {
            polled_payloads.insert(std::string(message.Payload().begin(), message.Payload().end()));
        }
    }

    EXPECT_EQ(polled_partitions, (std::unordered_set<std::uint32_t>{0, 1}));
    EXPECT_EQ(polled_payloads, (std::unordered_set<std::string>{"p0-0", "p0-1", "p1-0", "p1-1"}));
}
