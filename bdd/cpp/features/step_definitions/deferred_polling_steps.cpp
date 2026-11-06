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
// step files when linked together (see background_steps.cpp for details).
#define CUKE_OBJECT_PREFIX IggyBddDeferredPolling

#include <gtest/gtest.h>

#include <cucumber-cpp/autodetect.hpp>

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "world.hpp"

namespace {

constexpr std::uint32_t kMessagesCount = 10;
// A message that reaches the server before the call answers it at once, so a start step waits
// until its call is parked.
constexpr auto kParkTime = std::chrono::milliseconds(100);
// The wait of a consumer whose step names none.
constexpr auto kConsumerMaxWait            = std::chrono::seconds(10);
constexpr std::uint32_t kDefaultConsumerId = 1;
// How long a poll whose connection dropped may take to fail.
constexpr auto kLostPollTimeout                 = std::chrono::seconds(10);
constexpr std::uint32_t kPartitionLaneBlocked   = 14009;
constexpr std::uint32_t kFetchProgressUncertain = 14010;

// The SDK defaults with the count of these steps and the max wait of the step.
iggy::PollOptions poll_options(const std::chrono::microseconds max_wait) {
    return iggy::PollOptions().SetCount(kMessagesCount).SetMaxWait(max_wait);
}

std::string payload_text(const iggy::IggyMessagePolled &message) {
    return std::string(message.Payload().begin(), message.Payload().end());
}

std::vector<std::string> payloads_of(const iggy::PolledMessages &polled) {
    std::vector<std::string> payloads;
    for (const auto &message : polled.Messages()) {
        payloads.push_back(payload_text(message));
    }
    return payloads;
}

iggy::Identifier stream_of(const bdd::GlobalContext &context) {
    return iggy::Identifier::String(context.last_stream_name.value());
}

iggy::Identifier topic_of(const bdd::GlobalContext &context) {
    return iggy::Identifier::String(context.last_topic_name.value());
}

iggy::PendingPoll take_deferred_poll(bdd::GlobalContext &context) {
    iggy::PendingPoll poll = std::move(context.deferred_poll.value());
    context.deferred_poll.reset();
    return poll;
}

void start_poll(bdd::GlobalContext &context,
                const std::uint32_t partition_id,
                const iggy::Consumer &consumer,
                const iggy::PollingStrategy &strategy,
                const iggy::PollOptions &options) {
    if (!context.background) {
        context.background.emplace(bdd::connect_root_client());
    }
    // The first read of a new topic can wait for its partition, so a read that waits for nothing goes first and the
    // poll then leaves at once.
    (void)context.background->PollMessages(iggy::Consumer::Single(iggy::Identifier::Numeric(kDefaultConsumerId)),
                                           stream_of(context), topic_of(context), partition_id,
                                           iggy::PollingStrategy::Offset(0), iggy::PollOptions().SetCount(1));
    context.deferred_poll_started = std::chrono::steady_clock::now();
    context.deferred_poll.emplace(context.background->StartPollMessages(consumer, stream_of(context), topic_of(context),
                                                                        partition_id, strategy, options));
    std::this_thread::sleep_for(kParkTime);
    ASSERT_FALSE(context.deferred_poll->Wait(std::chrono::microseconds(0)).has_value())
        << "the deferred poll must park";
}

iggy::PolledMessages poll_next(bdd::GlobalContext &context,
                               const std::uint32_t partition_id,
                               const std::string &consumer_name,
                               const iggy::CommitPolicy commit) {
    // One message from next, so a poll that commits stores only the offset of the payload it returns.
    auto &client = context.poll_owner != nullptr ? *context.poll_owner : bdd::sdk_client(context);
    return client.PollMessages(iggy::Consumer::Single(iggy::Identifier::String(consumer_name)), stream_of(context),
                               topic_of(context), partition_id, iggy::PollingStrategy::Next(),
                               iggy::PollOptions().SetCount(1).SetCommit(commit));
}

void poll_next_and_expect(bdd::GlobalContext &context,
                          const std::uint32_t partition_id,
                          const std::string &consumer_name,
                          const iggy::CommitPolicy commit,
                          const std::string &payload) {
    const auto polled = poll_next(context, partition_id, consumer_name, commit);
    ASSERT_FALSE(polled.Messages().empty()) << "the poll must return the payload " << payload;
    EXPECT_EQ(payload_text(polled.Messages().front()), payload);
}

void open_group_consumer(bdd::GlobalContext &context, const std::string &group, const iggy::PollOptions &options) {
    context.consumer.emplace(bdd::sdk_client(context).GroupConsumer(group, stream_of(context), topic_of(context),
                                                                    iggy::ConsumerOptions().SetPollOptions(options)));
}

}  // namespace

GIVEN("^a stream named \"([^\"]+)\" with a topic named \"([^\"]+)\" with ([0-9]+) partitions exists$") {
    REGEX_PARAM(std::string, stream_name);
    REGEX_PARAM(std::string, topic_name);
    REGEX_PARAM(int, partitions);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    auto &client = bdd::sdk_client(*context);
    for (const auto &stream : client.GetStreams()) {
        if (stream.Name() == stream_name) {
            client.DeleteStream(iggy::Identifier::String(stream_name));
        }
    }
    client.CreateStream(stream_name);
    client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                       iggy::TopicCreateOptions().SetPartitionsCount(static_cast<std::uint32_t>(partitions)));
    context->last_stream_name = stream_name;
    context->last_topic_name  = topic_name;
}

WHEN("^I start a deferred poll of partition ([0-9]+) from offset ([0-9]+) with a max wait of ([0-9]+) seconds?$") {
    REGEX_PARAM(int, partition_id);
    REGEX_PARAM(int, offset);
    REGEX_PARAM(int, max_wait_seconds);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    start_poll(*context, static_cast<std::uint32_t>(partition_id),
               iggy::Consumer::Single(iggy::Identifier::Numeric(kDefaultConsumerId)),
               iggy::PollingStrategy::Offset(static_cast<std::uint64_t>(offset)),
               poll_options(std::chrono::seconds(max_wait_seconds)));
}

WHEN(
    "^I start a deferred poll of partition ([0-9]+) for consumer \"([^\"]+)\" with the on read commit policy, a min "
    "count of ([0-9]+) and a max wait of ([0-9]+) seconds?$") {
    REGEX_PARAM(int, partition_id);
    REGEX_PARAM(std::string, consumer_name);
    REGEX_PARAM(int, min_count);
    REGEX_PARAM(int, max_wait_seconds);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    start_poll(*context, static_cast<std::uint32_t>(partition_id),
               iggy::Consumer::Single(iggy::Identifier::String(consumer_name)), iggy::PollingStrategy::Next(),
               poll_options(std::chrono::seconds(max_wait_seconds))
                   .SetMinCount(static_cast<std::uint32_t>(min_count))
                   .SetCommit(iggy::CommitPolicy::OnRead));
}

WHEN(
    "^I start a deferred poll of partition ([0-9]+) for consumer \"([^\"]+)\" with the on read commit policy and a "
    "max wait of ([0-9]+) seconds?$") {
    REGEX_PARAM(int, partition_id);
    REGEX_PARAM(std::string, consumer_name);
    REGEX_PARAM(int, max_wait_seconds);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    start_poll(*context, static_cast<std::uint32_t>(partition_id),
               iggy::Consumer::Single(iggy::Identifier::String(consumer_name)), iggy::PollingStrategy::Next(),
               poll_options(std::chrono::seconds(max_wait_seconds)).SetCommit(iggy::CommitPolicy::OnRead));
}

WHEN("^the client disconnects before the deferred poll answers$") {
    cucumber::ScenarioScope<bdd::GlobalContext> context;
    ASSERT_TRUE(context->deferred_poll.has_value()) << "deferred poll started";
    ASSERT_FALSE(context->deferred_poll->Wait(std::chrono::microseconds(0)).has_value())
        << "the deferred poll must still wait";

    context->poll_owner = &context->background.value();
    context->poll_owner->Disconnect();
}

THEN("^the deferred poll should fail as uncertain$") {
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    auto poll = take_deferred_poll(*context);
    try {
        (void)poll.Wait(kLostPollTimeout);
        FAIL() << "the deferred poll must fail as uncertain";
    } catch (const iggy::IggyException &error) {
        EXPECT_EQ(error.Code(), kFetchProgressUncertain) << error.what();
        EXPECT_EQ(error.Kind(), "fetch_progress_uncertain");
        ASSERT_TRUE(error.OperationId().has_value()) << error.what();
        context->lost_operation_id = error.OperationId();
    }
}

WHEN("^the client connects again$") {
    cucumber::ScenarioScope<bdd::GlobalContext> context;
    ASSERT_NE(context->poll_owner, nullptr) << "the client disconnected";

    // The automatic login of the client signs it in again.
    context->poll_owner->Connect();
}

THEN(
    "^a poll of partition ([0-9]+) for consumer \"([^\"]+)\" with the next strategy and the on read commit policy "
    "should fail as blocked by the deferred poll$") {
    REGEX_PARAM(int, partition_id);
    REGEX_PARAM(std::string, consumer_name);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    try {
        (void)poll_next(*context, static_cast<std::uint32_t>(partition_id), consumer_name, iggy::CommitPolicy::OnRead);
        FAIL() << "the poll must fail as blocked";
    } catch (const iggy::IggyException &error) {
        EXPECT_EQ(error.Code(), kPartitionLaneBlocked) << error.what();
        EXPECT_EQ(error.Kind(), "partition_lane_blocked");
        EXPECT_EQ(error.BlockedPartitionId(), static_cast<std::uint32_t>(partition_id));
        EXPECT_EQ(error.OperationId(), context->lost_operation_id);
    }
}

WHEN("^I abandon the operation of the deferred poll$") {
    cucumber::ScenarioScope<bdd::GlobalContext> context;
    ASSERT_NE(context->poll_owner, nullptr) << "the client disconnected";
    ASSERT_TRUE(context->lost_operation_id.has_value()) << "the deferred poll failed as uncertain";

    context->poll_owner->AbandonOperation(*context->lost_operation_id);
}

WHEN("^I send a message with payload \"([^\"]+)\" to partition ([0-9]+)$") {
    iggy::ffi::ErrorInfo error_info{};
    REGEX_PARAM(std::string, payload);
    REGEX_PARAM(int, partition_id);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    messages.push_back(iggy::ffi::make_message(bdd::to_payload(payload), rust::Vec<iggy::ffi::HeaderEntry>{}));
    context->client->send_messages(bdd::make_string_identifier(context->last_stream_name.value()),
                                   bdd::make_string_identifier(context->last_topic_name.value()), "partition_id",
                                   bdd::partition_id_bytes(static_cast<std::uint32_t>(partition_id)),
                                   std::move(messages), error_info);
}

THEN("^the deferred poll should return the payload \"([^\"]+)\" within ([0-9]+) seconds?$") {
    REGEX_PARAM(std::string, payload);
    REGEX_PARAM(int, within_seconds);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    auto poll         = take_deferred_poll(*context);
    const auto polled = poll.Wait(std::chrono::seconds(within_seconds));
    ASSERT_TRUE(polled.has_value()) << "the deferred poll did not return within " << within_seconds << " s";
    EXPECT_EQ(payloads_of(*polled), std::vector<std::string>{payload});
}

THEN("^the deferred poll should return no messages between ([0-9]+) and ([0-9]+) seconds after it started$") {
    REGEX_PARAM(int, from_seconds);
    REGEX_PARAM(int, to_seconds);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    const auto started   = context->deferred_poll_started;
    auto poll            = take_deferred_poll(*context);
    const auto remaining = started + std::chrono::seconds(to_seconds) - std::chrono::steady_clock::now();
    const auto polled    = poll.Wait(std::chrono::duration_cast<std::chrono::microseconds>(remaining));
    const auto elapsed   = std::chrono::steady_clock::now() - started;
    ASSERT_TRUE(polled.has_value()) << "the deferred poll did not return within " << to_seconds << " s";
    EXPECT_TRUE(polled->Messages().empty()) << "the deferred poll must return no messages";
    EXPECT_GE(elapsed, std::chrono::seconds(from_seconds)) << "the deferred poll returned too early";
}

WHEN("^I cancel the deferred poll after ([0-9]+) milliseconds$") {
    REGEX_PARAM(int, after_milliseconds);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    std::this_thread::sleep_for(std::chrono::milliseconds(after_milliseconds));
    auto poll = take_deferred_poll(*context);
    ASSERT_FALSE(poll.Wait(std::chrono::microseconds(0)).has_value())
        << "the deferred poll must still be parked when it is cancelled";
    // Dropping the parked request sends the cancellation.
    poll.Cancel();
}

WHEN("^I wait ([0-9]+) seconds?$") {
    REGEX_PARAM(int, seconds);

    std::this_thread::sleep_for(std::chrono::seconds(seconds));
}

THEN(
    "^a poll of partition ([0-9]+) for consumer \"([^\"]+)\" with the next strategy should return the payload "
    "\"([^\"]+)\"$") {
    REGEX_PARAM(int, partition_id);
    REGEX_PARAM(std::string, consumer_name);
    REGEX_PARAM(std::string, payload);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    poll_next_and_expect(*context, static_cast<std::uint32_t>(partition_id), consumer_name, iggy::CommitPolicy::Manual,
                         payload);
}

THEN(
    "^a poll of partition ([0-9]+) for consumer \"([^\"]+)\" with the next strategy and the on read commit policy "
    "should return the payload \"([^\"]+)\"$") {
    REGEX_PARAM(int, partition_id);
    REGEX_PARAM(std::string, consumer_name);
    REGEX_PARAM(std::string, payload);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    poll_next_and_expect(*context, static_cast<std::uint32_t>(partition_id), consumer_name, iggy::CommitPolicy::OnRead,
                         payload);
}

WHEN(
    "^I open a partition reader on partitions ([0-9]+), ([0-9]+) and ([0-9]+) from offset ([0-9]+) with a max wait "
    "of ([0-9]+) seconds?$") {
    REGEX_PARAM(int, first);
    REGEX_PARAM(int, second);
    REGEX_PARAM(int, third);
    REGEX_PARAM(int, offset);
    REGEX_PARAM(int, max_wait_seconds);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    context->reader.emplace(bdd::sdk_client(*context).Reader(
        iggy::Consumer::Single(iggy::Identifier::Numeric(kDefaultConsumerId)), stream_of(*context), topic_of(*context),
        iggy::ReaderOptions()
            .SetPartitions({static_cast<std::uint32_t>(first), static_cast<std::uint32_t>(second),
                            static_cast<std::uint32_t>(third)})
            .SetStart(iggy::PollingStrategy::Offset(static_cast<std::uint64_t>(offset)))
            .SetPollOptions(poll_options(std::chrono::seconds(max_wait_seconds)))));
}

THEN("^the partition reader should return the payload \"([^\"]+)\" from partition ([0-9]+) within ([0-9]+) seconds?$") {
    REGEX_PARAM(std::string, payload);
    REGEX_PARAM(int, partition_id);
    REGEX_PARAM(int, within_seconds);
    cucumber::ScenarioScope<bdd::GlobalContext> context;
    ASSERT_TRUE(context->reader.has_value()) << "partition reader open";

    // The reader never yields an empty batch, so its first batch comes from the first partition
    // that gets a message.
    const auto polled = context->reader->Next(std::chrono::seconds(within_seconds));
    ASSERT_TRUE(polled.has_value()) << "the partition reader did not return within " << within_seconds << " s";
    EXPECT_EQ(polled->PartitionId(), static_cast<std::uint32_t>(partition_id));
    EXPECT_EQ(payloads_of(*polled), std::vector<std::string>{payload});
}

WHEN("^I open a consumer in group \"([^\"]+)\" on the topic with a max wait of ([0-9]+) seconds?$") {
    REGEX_PARAM(std::string, group);
    REGEX_PARAM(int, max_wait_seconds);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    open_group_consumer(*context, group, poll_options(std::chrono::seconds(max_wait_seconds)));
}

WHEN("^I open a consumer in group \"([^\"]+)\" on the topic that commits after processing$") {
    REGEX_PARAM(std::string, group);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    open_group_consumer(
        *context, group,
        poll_options(kConsumerMaxWait).SetAutoCommit(iggy::AutoCommit().SetAfter(iggy::CommitAfter::Processing)));
}

THEN("^the consumer should return the payload \"([^\"]+)\" within ([0-9]+) seconds?$") {
    REGEX_PARAM(std::string, payload);
    REGEX_PARAM(int, within_seconds);
    cucumber::ScenarioScope<bdd::GlobalContext> context;
    ASSERT_TRUE(context->consumer.has_value()) << "consumer open";

    const auto received = context->consumer->Next(std::chrono::seconds(within_seconds));
    ASSERT_TRUE(received.has_value()) << "the consumer did not return within " << within_seconds << " s";
    EXPECT_EQ(payload_text(received->Message()), payload);
}

WHEN("^I close the consumer$") {
    cucumber::ScenarioScope<bdd::GlobalContext> context;
    ASSERT_TRUE(context->consumer.has_value()) << "consumer open";

    context->consumer->Close();
    context->consumer.reset();
}
