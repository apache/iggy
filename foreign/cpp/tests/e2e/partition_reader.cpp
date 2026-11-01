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

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "iggy.hpp"
#include "tests/e2e/test_helpers.hpp"

namespace {

constexpr auto kReadTimeout            = std::chrono::seconds(10);
constexpr auto kIdleReadTimeout        = std::chrono::milliseconds(200);
constexpr std::uint32_t kMessagesCount = 10;
constexpr std::uint32_t kBatchCount    = 4;

std::vector<std::uint64_t> offsets_of(const iggy::PolledMessages &batch) {
    std::vector<std::uint64_t> offsets;
    for (const auto &message : batch.Messages()) {
        offsets.push_back(message.Offset());
    }
    return offsets;
}

iggy::ReaderOptions reader_options(const iggy::PollOptions &poll_options) {
    return iggy::ReaderOptions()
        .SetPartitions({0})
        .SetStart(iggy::PollingStrategy::Offset(0))
        .SetPollOptions(poll_options);
}

}  // namespace

class E2E_PartitionReader : public E2ETestFixture {
  protected:
    // One topic with one partition. The messages go to partition 0 and carry their index as payload.
    iggy::Identifier CreateTopicWithMessages(const std::uint32_t messages_count) {
        iggy::ffi::ErrorInfo error_info{};
        iggy::ffi::Client *client     = GetLoggedInClient();
        const std::string stream_name = GetRandomName();
        client->create_stream(stream_name, {}, error_info);
        stream_id_ = client->get_stream(make_string_identifier(stream_name)).id;
        TrackStream(stream_id_);
        client->create_topic(make_numeric_identifier(stream_id_), GetRandomName(), make_topic_create_options(1),
                             error_info);
        SendMessages(messages_count);
        return iggy::Identifier::Numeric(stream_id_);
    }

    void SendMessages(const std::uint32_t messages_count) {
        if (messages_count == 0) {
            return;
        }
        iggy::ffi::ErrorInfo error_info{};
        rust::Vec<iggy::ffi::IggyMessageToSend> messages;
        for (std::uint32_t i = 0; i < messages_count; i++) {
            messages.push_back(
                iggy::ffi::make_message(to_payload(std::to_string(i)), rust::Vec<iggy::ffi::HeaderEntry>()));
        }
        GetLoggedInClient()->send_messages(make_numeric_identifier(stream_id_), make_numeric_identifier(0),
                                           "partition_id", partition_id_bytes(0), std::move(messages), error_info);
    }

  private:
    std::uint32_t stream_id_ = 0;
};

TEST_F(E2E_PartitionReader, PollMessagesReadsOnFromTheContinuationOfABatch) {
    RecordProperty("description",
                   "Polls a batch, reads on from its continuation and checks its checkpoints. An empty batch has no "
                   "checkpoint.");
    auto client         = GetLoggedInHighLevelClient();
    const auto consumer = iggy::Consumer::Single(iggy::Identifier::Numeric(1));
    const auto stream   = CreateTopicWithMessages(kMessagesCount);
    const auto topic    = iggy::Identifier::Numeric(0);
    const auto options  = iggy::PollOptions().SetCount(kBatchCount);

    const auto first = client.PollMessages(consumer, stream, topic, 0, iggy::PollingStrategy::Offset(0), options);
    EXPECT_EQ(first.PartitionId(), 0u);
    EXPECT_EQ(first.CurrentOffset(), kMessagesCount - 1);
    EXPECT_EQ(offsets_of(first), (std::vector<std::uint64_t>{0, 1, 2, 3}));
    EXPECT_EQ(first.Checkpoint().Offset(), 3u);
    EXPECT_EQ(first.CheckpointAt(1).Offset(), 1u);
    EXPECT_EQ(first.CheckpointAt(1).PartitionId(), 0u);
    EXPECT_THROW((void)first.CheckpointAt(kBatchCount), iggy::IggyException);

    const auto continuation = first.Continuation();
    EXPECT_EQ(continuation.Kind(), "offset");
    EXPECT_EQ(continuation.Value(), 4u);
    const auto second = client.PollMessages(consumer, stream, topic, 0, continuation, options);
    EXPECT_EQ(offsets_of(second), (std::vector<std::uint64_t>{4, 5, 6, 7}));

    const auto empty =
        client.PollMessages(consumer, stream, topic, 0, iggy::PollingStrategy::Offset(kMessagesCount), options);
    EXPECT_TRUE(empty.Messages().empty());
    EXPECT_THROW((void)empty.Checkpoint(), iggy::IggyException);
}

TEST_F(E2E_PartitionReader, PollMessagesRefusesAutoCommit) {
    RecordProperty("description", "A one-shot poll refuses the auto commit policy, which only a reader runs.");
    auto client       = GetLoggedInHighLevelClient();
    const auto stream = CreateTopicWithMessages(1);

    try {
        (void)client.PollMessages(iggy::Consumer::Single(iggy::Identifier::Numeric(1)), stream,
                                  iggy::Identifier::Numeric(0), 0, iggy::PollingStrategy::Offset(0),
                                  iggy::PollOptions().SetAutoCommit(iggy::AutoCommit()));
        FAIL() << "a one-shot poll must refuse CommitPolicy::Auto";
    } catch (const iggy::IggyException &error) {
        EXPECT_EQ(error.Kind(), "invalid_configuration");
    }
}

TEST_F(E2E_PartitionReader, ReaderReadsEveryMessageAndCommitStoresTheOffset) {
    RecordProperty("description",
                   "Reads every message of one partition with a reader, commits the last checkpoint and reads the "
                   "stored offset back. Every call but Close throws after Close.");
    auto client         = GetLoggedInHighLevelClient();
    const auto consumer = iggy::Consumer::Single(iggy::Identifier::Numeric(2));
    const auto stream   = CreateTopicWithMessages(kMessagesCount);
    const auto topic    = iggy::Identifier::Numeric(0);
    auto reader = client.Reader(consumer, stream, topic, reader_options(iggy::PollOptions().SetCount(kBatchCount)));
    EXPECT_EQ(reader.Partitions(), (std::vector<std::uint32_t>{0}));

    std::vector<std::uint64_t> offsets;
    std::optional<iggy::Checkpoint> last;
    while (offsets.size() < kMessagesCount) {
        auto batch = reader.Next(kReadTimeout);
        ASSERT_TRUE(batch.has_value()) << "the reader must deliver every message";
        EXPECT_EQ(batch->PartitionId(), 0u);
        for (const auto offset : offsets_of(*batch)) {
            offsets.push_back(offset);
        }
        last.emplace(batch->Checkpoint());
    }
    EXPECT_EQ(offsets, (std::vector<std::uint64_t>{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
    EXPECT_FALSE(reader.Next(kIdleReadTimeout).has_value());

    reader.Commit(*last);
    EXPECT_EQ(client.GetConsumerOffset(consumer, stream, topic, 0).StoredOffset(), kMessagesCount - 1);

    reader.Close();
    reader.Close();
    EXPECT_THROW((void)reader.Next(kIdleReadTimeout), iggy::IggyException);
    EXPECT_THROW(reader.Commit(*last), iggy::IggyException);
}

TEST_F(E2E_PartitionReader, SeekReadsThePartitionAgain) {
    RecordProperty("description",
                   "Seeks a partition back to its first message, then to the continuation of a batch, and reads from "
                   "each new start.");
    auto client       = GetLoggedInHighLevelClient();
    const auto stream = CreateTopicWithMessages(kMessagesCount);
    auto reader =
        client.Reader(iggy::Consumer::Single(iggy::Identifier::Numeric(3)), stream, iggy::Identifier::Numeric(0),
                      reader_options(iggy::PollOptions().SetCount(kBatchCount)));

    auto first = reader.Next(kReadTimeout);
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(offsets_of(*first), (std::vector<std::uint64_t>{0, 1, 2, 3}));

    reader.Seek(0, iggy::PollingStrategy::First());
    auto again = reader.Next(kReadTimeout);
    ASSERT_TRUE(again.has_value());
    EXPECT_EQ(offsets_of(*again), (std::vector<std::uint64_t>{0, 1, 2, 3}));

    reader.Seek(0, again->Continuation());
    auto next = reader.Next(kReadTimeout);
    ASSERT_TRUE(next.has_value());
    EXPECT_EQ(offsets_of(*next), (std::vector<std::uint64_t>{4, 5, 6, 7}));
    reader.Close();
}

TEST_F(E2E_PartitionReader, CommitIsRefusedWhenTheServerCommitsOnRead) {
    RecordProperty("description", "A reader with the on read policy refuses a commit, and Mark needs auto commit.");
    auto client       = GetLoggedInHighLevelClient();
    const auto stream = CreateTopicWithMessages(kMessagesCount);
    auto reader =
        client.Reader(iggy::Consumer::Single(iggy::Identifier::Numeric(4)), stream, iggy::Identifier::Numeric(0),
                      reader_options(iggy::PollOptions().SetCount(kBatchCount).SetCommit(iggy::CommitPolicy::OnRead)));

    auto batch = reader.Next(kReadTimeout);
    ASSERT_TRUE(batch.has_value());
    EXPECT_THROW(reader.Commit(batch->Checkpoint()), iggy::IggyException);
    EXPECT_THROW(reader.Mark(batch->Checkpoint()), iggy::IggyException);
    reader.Close();
}

TEST_F(E2E_PartitionReader, OnReadCloseMovesTheOffsetBackAndASecondSessionResumesAfterIt) {
    RecordProperty("description",
                   "A reader with the on read policy that delivered one batch of a prefetched partition moves the "
                   "stored offset back to that batch in Close, and a reader of another session resumes after it.");
    auto client         = GetLoggedInHighLevelClient();
    const auto consumer = iggy::Consumer::Single(iggy::Identifier::Numeric(9));
    const auto stream   = CreateTopicWithMessages(kMessagesCount);
    const auto topic    = iggy::Identifier::Numeric(0);
    const auto on_read  = iggy::PollOptions().SetCount(kBatchCount).SetCommit(iggy::CommitPolicy::OnRead);
    auto reader         = client.Reader(consumer, stream, topic, reader_options(on_read));

    const auto batch = reader.Next(kReadTimeout);
    ASSERT_TRUE(batch.has_value());
    EXPECT_EQ(offsets_of(*batch), (std::vector<std::uint64_t>{0, 1, 2, 3}));
    reader.Close();
    EXPECT_EQ(client.GetConsumerOffset(consumer, stream, topic, 0).StoredOffset(), kBatchCount - 1);

    auto second_session = GetLoggedInHighLevelClient();
    auto resumed        = second_session.Reader(consumer, stream, topic,
                                                iggy::ReaderOptions().SetPartitions({0}).SetPollOptions(on_read));
    const auto next     = resumed.Next(kReadTimeout);
    ASSERT_TRUE(next.has_value());
    EXPECT_EQ(offsets_of(*next), (std::vector<std::uint64_t>{4, 5, 6, 7}));
    resumed.Close();
}

TEST_F(E2E_PartitionReader, AutoCommitStoresTheMarkedPositionWhenTheReaderCloses) {
    RecordProperty("description",
                   "Under auto commit after marked, Close stores the latest marked checkpoint and nothing past it.");
    auto client         = GetLoggedInHighLevelClient();
    const auto consumer = iggy::Consumer::Single(iggy::Identifier::Numeric(5));
    const auto stream   = CreateTopicWithMessages(kMessagesCount);
    const auto topic    = iggy::Identifier::Numeric(0);
    const auto options =
        iggy::PollOptions()
            .SetCount(kBatchCount)
            .SetAutoCommit(iggy::AutoCommit().SetEvery(std::nullopt).SetAfter(iggy::CommitAfter::Marked));
    auto reader = client.Reader(consumer, stream, topic, reader_options(options));

    auto batch = reader.Next(kReadTimeout);
    ASSERT_TRUE(batch.has_value());
    reader.Mark(batch->CheckpointAt(1));
    reader.Close();

    EXPECT_EQ(client.GetConsumerOffset(consumer, stream, topic, 0).StoredOffset(), 1u);
}

TEST_F(E2E_PartitionReader, ConsumerGroupReaderReadsItsAssignment) {
    RecordProperty("description",
                   "A consumer group reader joins in Reader, reads the assigned partition and leaves in Close.");
    auto client                  = GetLoggedInHighLevelClient();
    const auto stream            = CreateTopicWithMessages(kMessagesCount);
    const auto topic             = iggy::Identifier::Numeric(0);
    const std::string group_name = GetRandomName();
    client.CreateConsumerGroup(stream, topic, group_name);
    const auto consumer = iggy::Consumer::Group(iggy::Identifier::String(group_name));

    auto reader = client.Reader(consumer, stream, topic,
                                iggy::ReaderOptions()
                                    .SetStart(iggy::PollingStrategy::Offset(0))
                                    .SetPollOptions(iggy::PollOptions().SetCount(kMessagesCount)));
    auto batch  = reader.Next(kReadTimeout);
    ASSERT_TRUE(batch.has_value());
    EXPECT_EQ(batch->Messages().size(), kMessagesCount);
    // The assignment arrives with the reads, so it is checked after the first batch.
    EXPECT_EQ(reader.Partitions(), (std::vector<std::uint32_t>{0}));
    reader.Commit(batch->Checkpoint());
    reader.Close();

    EXPECT_EQ(client.GetConsumerOffset(consumer, stream, topic, 0).StoredOffset(), kMessagesCount - 1);
    EXPECT_EQ(client.GetConsumerGroup(stream, topic, iggy::Identifier::String(group_name)).MembersCount(), 0u);
}

TEST_F(E2E_PartitionReader, PendingPollReturnsTheBatchOfAParkedPollWhenAMessageArrives) {
    RecordProperty("description",
                   "A deferred poll started in the background stays parked on an empty partition and returns the "
                   "message that arrives. A second wait is refused.");
    auto client       = GetLoggedInHighLevelClient();
    const auto stream = CreateTopicWithMessages(0);
    auto poll         = client.StartPollMessages(iggy::Consumer::Single(iggy::Identifier::Numeric(6)), stream,
                                                 iggy::Identifier::Numeric(0), 0, iggy::PollingStrategy::Offset(0),
                                                 iggy::PollOptions().SetMaxWait(kReadTimeout));

    EXPECT_FALSE(poll.Wait(kIdleReadTimeout).has_value());
    SendMessages(1);
    const auto batch = poll.Wait(kReadTimeout);
    ASSERT_TRUE(batch.has_value());
    EXPECT_EQ(offsets_of(*batch), (std::vector<std::uint64_t>{0}));
    EXPECT_THROW((void)poll.Wait(kIdleReadTimeout), iggy::IggyException);
}

TEST_F(E2E_PartitionReader, CancelledPollThatCommitsOnReadStoresNoOffset) {
    RecordProperty("description",
                   "A parked deferred poll with the on read policy that is cancelled before it answers stores no "
                   "offset, and a wait after the cancel is refused.");
    auto client         = GetLoggedInHighLevelClient();
    const auto consumer = iggy::Consumer::Single(iggy::Identifier::Numeric(7));
    const auto stream   = CreateTopicWithMessages(1);
    const auto topic    = iggy::Identifier::Numeric(0);
    auto poll           = client.StartPollMessages(
        consumer, stream, topic, 0, iggy::PollingStrategy::Next(),
        iggy::PollOptions().SetMinCount(2).SetMaxWait(std::chrono::seconds(1)).SetCommit(iggy::CommitPolicy::OnRead));

    EXPECT_FALSE(poll.Wait(kIdleReadTimeout).has_value());
    poll.Cancel();
    poll.Cancel();
    EXPECT_THROW((void)poll.Wait(kIdleReadTimeout), iggy::IggyException);

    // Past the max wait, after which an uncancelled poll would have stored its offset.
    std::this_thread::sleep_for(std::chrono::seconds(2));
    EXPECT_THROW((void)client.GetConsumerOffset(consumer, stream, topic, 0), iggy::IggyException);
}

TEST_F(E2E_PartitionReader, StandaloneConsumerHandsOverEveryMessageAndStoresTheLastOneOnClose) {
    RecordProperty("description",
                   "A standalone consumer under auto commit after processing hands over every message and Close "
                   "stores the last one. Every call but Close throws after Close.");
    auto client            = GetLoggedInHighLevelClient();
    const auto stream      = CreateTopicWithMessages(kMessagesCount);
    const auto topic       = iggy::Identifier::Numeric(0);
    const std::string name = GetRandomName();
    auto consumer          = client.StandaloneConsumer(
        name, stream, topic,
        iggy::ConsumerOptions()
            .SetPartitions({0})
            .SetStart(iggy::PollingStrategy::Offset(0))
            .SetPollOptions(iggy::PollOptions().SetCount(kBatchCount).SetAutoCommit(iggy::AutoCommit())));

    std::vector<std::uint64_t> offsets;
    while (offsets.size() < kMessagesCount) {
        const auto received = consumer.Next(kReadTimeout);
        ASSERT_TRUE(received.has_value()) << "the consumer must hand over every message";
        EXPECT_EQ(received->PartitionId(), 0u);
        EXPECT_EQ(received->Checkpoint().Offset(), received->Message().Offset());
        offsets.push_back(received->Message().Offset());
    }
    EXPECT_EQ(offsets, (std::vector<std::uint64_t>{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
    consumer.Close();
    consumer.Close();
    EXPECT_THROW((void)consumer.Next(kIdleReadTimeout), iggy::IggyException);

    EXPECT_EQ(client.GetConsumerOffset(iggy::Consumer::Single(iggy::Identifier::String(name)), stream, topic, 0)
                  .StoredOffset(),
              kMessagesCount - 1);
}

TEST_F(E2E_PartitionReader, GroupConsumerReadsItsAssignmentAndLeavesTheGroupOnClose) {
    RecordProperty("description",
                   "A group consumer creates and joins its group, commits a checkpoint it handed over and leaves the "
                   "group in Close. Mark needs auto commit.");
    auto client                  = GetLoggedInHighLevelClient();
    const auto stream            = CreateTopicWithMessages(kMessagesCount);
    const auto topic             = iggy::Identifier::Numeric(0);
    const std::string group_name = GetRandomName();
    auto consumer                = client.GroupConsumer(group_name, stream, topic,
                                                        iggy::ConsumerOptions().SetStart(iggy::PollingStrategy::Offset(0)));
    EXPECT_EQ(client.GetConsumerGroup(stream, topic, iggy::Identifier::String(group_name)).MembersCount(), 1u);

    std::optional<iggy::ReceivedMessage> last;
    for (std::uint32_t i = 0; i < kMessagesCount; i++) {
        last = consumer.Next(kReadTimeout);
        ASSERT_TRUE(last.has_value()) << "the consumer must hand over every message";
    }
    EXPECT_EQ(last->Message().Offset(), kMessagesCount - 1);
    EXPECT_THROW(consumer.Mark(last->Checkpoint()), iggy::IggyException);
    consumer.Commit(last->Checkpoint());
    consumer.Close();

    const auto group = iggy::Consumer::Group(iggy::Identifier::String(group_name));
    EXPECT_EQ(client.GetConsumerOffset(group, stream, topic, 0).StoredOffset(), kMessagesCount - 1);
    EXPECT_EQ(client.GetConsumerGroup(stream, topic, iggy::Identifier::String(group_name)).MembersCount(), 0u);
}

TEST_F(E2E_PartitionReader, ConsumerSeeksToAContinuationAndRefusesAnotherPartition) {
    auto client       = GetLoggedInHighLevelClient();
    const auto stream = CreateTopicWithMessages(kMessagesCount);
    const auto topic  = iggy::Identifier::Numeric(0);
    const auto batch  = client.PollMessages(iggy::Consumer::Single(iggy::Identifier::Numeric(8)), stream, topic, 0,
                                            iggy::PollingStrategy::Offset(0), iggy::PollOptions().SetCount(2));
    const auto continuation = batch.Continuation();
    auto consumer           = client.StandaloneConsumer(
        GetRandomName(), stream, topic,
        iggy::ConsumerOptions().SetPartitions({0}).SetStart(iggy::PollingStrategy::Offset(0)));

    consumer.Seek(0, continuation);
    const auto received = consumer.Next(kReadTimeout);
    ASSERT_TRUE(received.has_value());
    EXPECT_EQ(received->Message().Offset(), 2u);
    EXPECT_THROW(consumer.Seek(1, continuation), iggy::IggyException);
    consumer.Close();
}
