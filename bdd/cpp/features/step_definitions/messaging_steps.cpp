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

// Unique step-class prefix so the generated CukeObject<n> symbols do not clash with the other
// step file when linked together (see background_steps.cpp for details).
#define CUKE_OBJECT_PREFIX IggyBddMessaging

#include <gtest/gtest.h>

#include <cucumber-cpp/autodetect.hpp>

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>

#include "iggy.hpp"
#include "world.hpp"

GIVEN("^I have no streams in the system$") {
    cucumber::ScenarioScope<bdd::GlobalContext> context;
    ASSERT_NE(context->client, nullptr);

    const auto streams = context->client->get_streams();
    EXPECT_EQ(streams.size(), static_cast<std::size_t>(0));
}

WHEN("^I create a stream with name \"([^\"]{1,255})\"$") {
    REGEX_PARAM(std::string, name);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    context->client->create_stream(name);
}

THEN("^the stream should be created successfully$") {
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    const auto streams = context->client->get_streams();
    EXPECT_EQ(streams.size(), static_cast<std::size_t>(1));
}

THEN("^the stream should have name \"([^\"]{1,255})\"$") {
    REGEX_PARAM(std::string, name);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    const auto stream = context->client->get_stream(bdd::make_numeric_identifier(0));
    EXPECT_EQ(std::string(stream.name), name);
}

WHEN("^I create a topic with name \"([^\"]{1,255})\" in stream ([0-9]+) with ([0-9]+) partitions$") {
    REGEX_PARAM(std::string, topic_name);
    REGEX_PARAM(int, stream_id);
    REGEX_PARAM(int, partitions_count);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    // Drive the SDK through the typed helpers in iggy.hpp rather than raw strings. Options that
    // still take plain strings below (partitioning kind, consumer kind) mark where the C++ SDK
    // does not yet expose a typed wrapper.
    const auto compression    = iggy::CompressionAlgorithm::none();
    const auto message_expiry = iggy::Expiry::never_expire();
    const auto max_topic_size = iggy::MaxTopicSize::server_default();

    context->client->create_topic(bdd::make_numeric_identifier(static_cast<std::uint32_t>(stream_id)), topic_name,
                                  static_cast<std::uint32_t>(partitions_count),
                                  std::string(compression.compression_algorithm_value()), 0,
                                  std::string(message_expiry.expiry_kind()), message_expiry.expiry_value(),
                                  std::string(max_topic_size.max_topic_size()));
}

THEN("^the topic should be created successfully$") {
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    const auto stream = context->client->get_stream(bdd::make_numeric_identifier(0));
    EXPECT_FALSE(stream.topics.empty());
}

THEN("^the topic should have name \"([^\"]{1,255})\"$") {
    REGEX_PARAM(std::string, name);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    const auto stream = context->client->get_stream(bdd::make_numeric_identifier(0));
    ASSERT_FALSE(stream.topics.empty());
    EXPECT_EQ(std::string(stream.topics[0].name), name);
}

THEN("^the topic should have ([0-9]+) partitions$") {
    REGEX_PARAM(int, partitions_count);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    const auto stream = context->client->get_stream(bdd::make_numeric_identifier(0));
    ASSERT_FALSE(stream.topics.empty());
    EXPECT_EQ(stream.topics[0].partitions_count, static_cast<std::uint32_t>(partitions_count));
}

WHEN("^I send ([0-9]+) messages to stream ([0-9]+), topic ([0-9]+), partition ([0-9]+)$") {
    REGEX_PARAM(int, message_count);
    REGEX_PARAM(int, stream_id);
    REGEX_PARAM(int, topic_id);
    REGEX_PARAM(int, partition_id);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    for (int index = 0; index < message_count; ++index) {
        iggy::ffi::IggyMessageToSend message =
            iggy::ffi::make_message(bdd::to_payload(bdd::expected_payload(static_cast<std::uint32_t>(index))));
        // Assign an explicit, 1-based id so the last-sent/last-polled comparison is meaningful.
        message.id_lo = static_cast<std::uint64_t>(index + 1);
        messages.push_back(std::move(message));
    }

    const auto sent = messages.size();
    context->client->send_messages(bdd::make_numeric_identifier(static_cast<std::uint32_t>(stream_id)),
                                   bdd::make_numeric_identifier(static_cast<std::uint32_t>(topic_id)), "partition_id",
                                   bdd::partition_id_bytes(static_cast<std::uint32_t>(partition_id)),
                                   std::move(messages));

    context->sent_count        = static_cast<std::uint32_t>(sent);
    context->messages_to_send  = static_cast<std::uint32_t>(message_count);
    context->last_sent_payload = bdd::expected_payload(static_cast<std::uint32_t>(message_count - 1));
    context->last_sent_id_lo   = static_cast<std::uint64_t>(message_count);
}

THEN("^all messages should be sent successfully$") {
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    EXPECT_EQ(context->sent_count, context->messages_to_send);
}

WHEN("^I poll messages from stream ([0-9]+), topic ([0-9]+), partition ([0-9]+) starting from offset ([0-9]+)$") {
    REGEX_PARAM(int, stream_id);
    REGEX_PARAM(int, topic_id);
    REGEX_PARAM(int, partition_id);
    REGEX_PARAM(int, offset);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    const auto polling_strategy = iggy::PollingStrategy::offset(static_cast<std::uint64_t>(offset));
    const auto polled           = context->client->poll_messages(
        bdd::make_numeric_identifier(static_cast<std::uint32_t>(stream_id)),
        bdd::make_numeric_identifier(static_cast<std::uint32_t>(topic_id)), static_cast<std::uint32_t>(partition_id),
        "consumer", bdd::make_numeric_identifier(1), std::string(polling_strategy.polling_strategy_kind()),
        polling_strategy.polling_strategy_value(), 100, false);

    context->polled.count          = polled.count;
    context->polled.current_offset = polled.current_offset;
    context->polled.offsets.clear();
    context->polled.id_los.clear();
    context->polled.payloads.clear();
    for (const auto &message : polled.messages) {
        context->polled.offsets.push_back(message.offset);
        context->polled.id_los.push_back(message.id_lo);
        context->polled.payloads.push_back(bdd::payload_to_string(message.payload));
    }
}

THEN("^I should receive ([0-9]+) messages$") {
    REGEX_PARAM(int, expected_count);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    EXPECT_EQ(context->polled.count, static_cast<std::uint32_t>(expected_count));
    EXPECT_EQ(context->polled.payloads.size(), static_cast<std::size_t>(expected_count));
}

THEN("^the messages should have sequential offsets from ([0-9]+) to ([0-9]+)$") {
    REGEX_PARAM(int, from);
    REGEX_PARAM(int, to);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    ASSERT_EQ(context->polled.offsets.size(), static_cast<std::size_t>(to - from + 1));
    for (int offset = from; offset <= to; ++offset) {
        EXPECT_EQ(context->polled.offsets[static_cast<std::size_t>(offset - from)], static_cast<std::uint64_t>(offset));
    }
}

THEN("^each message should have the expected payload content$") {
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    for (std::size_t index = 0; index < context->polled.payloads.size(); ++index) {
        EXPECT_EQ(context->polled.payloads[index], bdd::expected_payload(static_cast<std::uint32_t>(index)));
    }
}

THEN("^the last polled message should match the last sent message$") {
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    ASSERT_FALSE(context->polled.payloads.empty());
    EXPECT_EQ(context->polled.payloads.back(), context->last_sent_payload);
    EXPECT_EQ(context->polled.id_los.back(), context->last_sent_id_lo);
}
