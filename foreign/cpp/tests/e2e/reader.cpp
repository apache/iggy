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
#include <vector>

#include <gtest/gtest.h>

#include "iggy.hpp"
#include "tests/e2e/test_helpers.hpp"

namespace {

constexpr auto kReadTimeout            = std::chrono::seconds(10);
constexpr auto kIdleReadTimeout        = std::chrono::milliseconds(200);
constexpr std::uint32_t kMessagesCount = 10;
constexpr std::uint32_t kBatchCount    = 4;

iggy::ReaderOptions standalone_options(const std::uint32_t partition_id, const iggy::CommitPolicy commit) {
    return iggy::ReaderOptions()
        .SetPartitions({partition_id})
        .SetStart(iggy::PollingStrategy::Offset(0))
        .SetPollOptions(iggy::PollOptions().SetCount(kBatchCount).SetCommit(commit));
}

}  // namespace

class E2E_Reader : public E2ETestFixture {
  protected:
    iggy::Identifier CreateTopicWithMessages(iggy::IggyBlockingClient &client, const std::uint32_t messages_count) {
        const auto stream = client.CreateStream(GetRandomName());
        TrackStream(stream.Id());
        const auto stream_id = iggy::Identifier::Numeric(stream.Id());
        client.CreateTopic(stream_id, GetRandomName(), iggy::TopicCreateOptions().SetPartitionsCount(1));
        if (messages_count > 0) {
            std::vector<iggy::IggyMessageToSend> messages;
            messages.reserve(messages_count);
            for (std::uint32_t index = 0; index < messages_count; ++index) {
                messages.push_back(iggy::IggyMessageToSend::Create(std::to_string(index)));
            }
            client.SendMessages(stream_id, iggy::Identifier::Numeric(0), iggy::Partitioning::PartitionId(0), messages);
        }
        return stream_id;
    }
};

TEST_F(E2E_Reader, ReadsEveryMessageAndCommitStoresTheOffset) {
    RecordProperty("description",
                   "Reads every message of one partition with a reader, commits the last checkpoint and reads the "
                   "stored offset back.");
    auto client           = GetLoggedInHighLevelClient();
    const auto stream     = CreateTopicWithMessages(client, kMessagesCount);
    const auto topic      = iggy::Identifier::Numeric(0);
    const auto consumer   = iggy::Consumer::Single(iggy::Identifier::Numeric(1));
    auto reader           = client.Reader(consumer, stream, topic, standalone_options(0, iggy::CommitPolicy::Manual));
    const auto partitions = reader.Partitions();
    ASSERT_EQ(partitions.size(), 1u);
    EXPECT_EQ(partitions[0], 0u);

    std::vector<std::uint64_t> offsets;
    offsets.reserve(kMessagesCount);
    std::optional<iggy::Checkpoint> last;
    while (offsets.size() < kMessagesCount) {
        const auto batch = reader.Next(kReadTimeout);
        ASSERT_TRUE(batch.has_value()) << "No batch before the timeout after " << offsets.size() << " messages";
        ASSERT_FALSE(batch->Messages().empty());
        EXPECT_EQ(batch->PartitionId(), 0u);
        for (const auto &message : batch->Messages()) {
            offsets.push_back(message.Offset());
        }
        last = batch->Checkpoint();
    }
    for (std::uint32_t index = 0; index < kMessagesCount; ++index) {
        EXPECT_EQ(offsets[index], static_cast<std::uint64_t>(index)) << "Offset mismatch at index " << index;
    }
    ASSERT_TRUE(last.has_value());
    EXPECT_EQ(last->PartitionId(), 0u);
    EXPECT_EQ(last->Offset(), kMessagesCount - 1);

    ASSERT_NO_THROW(reader.Commit(*last));
    const auto stored = client.GetConsumerOffset(consumer, stream, topic, 0);
    EXPECT_EQ(stored.StoredOffset(), kMessagesCount - 1);
    ASSERT_NO_THROW(reader.Close());
}

TEST_F(E2E_Reader, SeekReadsThePartitionAgainFromTheNewStart) {
    RecordProperty("description",
                   "Reads one batch, seeks back to offset 0 and expects the next batch to start there. Then reads on, "
                   "seeks to the continuation of the first batch and expects the next batch to start after it.");
    auto client         = GetLoggedInHighLevelClient();
    const auto stream   = CreateTopicWithMessages(client, kMessagesCount);
    const auto topic    = iggy::Identifier::Numeric(0);
    const auto consumer = iggy::Consumer::Single(iggy::Identifier::Numeric(1));
    auto reader         = client.Reader(consumer, stream, topic, standalone_options(0, iggy::CommitPolicy::Manual));

    const auto first = reader.Next(kReadTimeout);
    ASSERT_TRUE(first.has_value());
    ASSERT_FALSE(first->Messages().empty());
    EXPECT_EQ(first->Messages().front().Offset(), 0u);

    ASSERT_NO_THROW(reader.Seek(0, iggy::PollingStrategy::Offset(0)));
    const auto again = reader.Next(kReadTimeout);
    ASSERT_TRUE(again.has_value());
    ASSERT_FALSE(again->Messages().empty());
    EXPECT_EQ(again->Messages().front().Offset(), 0u) << "A batch queued before the seek was delivered after it";

    const auto later = reader.Next(kReadTimeout);
    ASSERT_TRUE(later.has_value());
    ASSERT_FALSE(later->Messages().empty());
    ASSERT_NO_THROW(reader.Seek(0, first->Continuation()));
    const auto resumed = reader.Next(kReadTimeout);
    ASSERT_TRUE(resumed.has_value());
    ASSERT_FALSE(resumed->Messages().empty());
    EXPECT_EQ(resumed->Messages().front().Offset(), first->Messages().back().Offset() + 1)
        << "A seek to a continuation must read on after its batch";

    try {
        reader.Seek(1, iggy::PollingStrategy::Offset(0));
        FAIL() << "A seek of a partition that the reader does not read must fail";
    } catch (const iggy::IggyException &error) {
        EXPECT_EQ(error.Kind(), "invalid_command");
    }
    ASSERT_NO_THROW(reader.Close());
}

TEST_F(E2E_Reader, NextReturnsNoBatchWhenTheTimeRunsOut) {
    RecordProperty("description", "Waits for the next batch of an empty partition and gets no batch.");
    auto client         = GetLoggedInHighLevelClient();
    const auto stream   = CreateTopicWithMessages(client, 0);
    const auto topic    = iggy::Identifier::Numeric(0);
    const auto consumer = iggy::Consumer::Single(iggy::Identifier::Numeric(1));
    auto reader         = client.Reader(consumer, stream, topic, standalone_options(0, iggy::CommitPolicy::Manual));

    const auto batch = reader.Next(kIdleReadTimeout);
    EXPECT_FALSE(batch.has_value());
    const auto empty = client.PollMessages(consumer, stream, topic, 0, iggy::PollingStrategy::Offset(0),
                                           iggy::PollOptions().SetCount(kBatchCount));
    EXPECT_TRUE(empty.Messages().empty());
    EXPECT_EQ(empty.Count(), 0u);
    EXPECT_THROW((void)empty.Checkpoint(), iggy::IggyException);
    ASSERT_NO_THROW(reader.Close());
}

TEST_F(E2E_Reader, CommitIsRefusedWhenTheServerCommitsOnRead) {
    RecordProperty("description", "A reader whose server commits on read refuses a commit from the caller.");
    auto client         = GetLoggedInHighLevelClient();
    const auto stream   = CreateTopicWithMessages(client, kMessagesCount);
    const auto consumer = iggy::Consumer::Single(iggy::Identifier::Numeric(1));
    auto reader         = client.Reader(consumer, stream, iggy::Identifier::Numeric(0),
                                        standalone_options(0, iggy::CommitPolicy::OnRead));

    const auto batch = reader.Next(kReadTimeout);
    ASSERT_TRUE(batch.has_value());
    ASSERT_FALSE(batch->Messages().empty());
    try {
        reader.Commit(batch->Checkpoint());
        FAIL() << "An on-read commit policy must refuse a manual commit";
    } catch (const iggy::IggyException &error) {
        EXPECT_EQ(error.Kind(), "invalid_command");
    }
    ASSERT_NO_THROW(reader.Close());
}

TEST_F(E2E_Reader, EveryCallButCloseThrowsAfterClose) {
    RecordProperty("description", "A closed reader refuses every later call, and a second close does nothing.");
    auto client         = GetLoggedInHighLevelClient();
    const auto stream   = CreateTopicWithMessages(client, kMessagesCount);
    const auto consumer = iggy::Consumer::Single(iggy::Identifier::Numeric(1));
    auto reader         = client.Reader(consumer, stream, iggy::Identifier::Numeric(0),
                                        standalone_options(0, iggy::CommitPolicy::Manual));
    const auto batch    = reader.Next(kReadTimeout);
    ASSERT_TRUE(batch.has_value());
    ASSERT_FALSE(batch->Messages().empty());

    ASSERT_NO_THROW(reader.Close());
    EXPECT_THROW((void)reader.Next(kIdleReadTimeout), iggy::IggyException);
    EXPECT_THROW(reader.Commit(batch->Checkpoint()), iggy::IggyException);
    EXPECT_THROW(reader.Seek(0, iggy::PollingStrategy::Offset(0)), iggy::IggyException);
    EXPECT_THROW(reader.Seek(0, batch->Continuation()), iggy::IggyException);
    EXPECT_THROW((void)reader.Partitions(), iggy::IggyException);
    EXPECT_NO_THROW(reader.Close());
}

TEST_F(E2E_Reader, ConsumerGroupReaderJoinsInBuildAndLeavesInClose) {
    RecordProperty("description",
                   "A group reader joins its group when it is built, reads the assigned partition and leaves the "
                   "group when it is closed.");
    auto client         = GetLoggedInHighLevelClient();
    const auto stream   = CreateTopicWithMessages(client, kMessagesCount);
    const auto topic    = iggy::Identifier::Numeric(0);
    const auto group    = client.CreateConsumerGroup(stream, topic, GetRandomName());
    const auto group_id = iggy::Identifier::Numeric(group.Id());
    auto reader         = client.Reader(iggy::Consumer::Group(group_id), stream, topic,
                                        iggy::ReaderOptions().SetStart(iggy::PollingStrategy::Offset(0)));
    EXPECT_EQ(client.GetConsumerGroup(stream, topic, group_id).MembersCount(), 1u);

    const auto batch = reader.Next(kReadTimeout);
    ASSERT_TRUE(batch.has_value());
    ASSERT_FALSE(batch->Messages().empty());
    EXPECT_EQ(batch->PartitionId(), 0u);
    EXPECT_EQ(batch->Messages().front().Offset(), 0u);

    ASSERT_NO_THROW(reader.Close());
    EXPECT_EQ(client.GetConsumerGroup(stream, topic, group_id).MembersCount(), 0u);
}

TEST_F(E2E_Reader, AutoCommitStoresTheMarkedPositionWhenTheReaderCloses) {
    RecordProperty("description",
                   "Marks the second message of a batch under CommitPolicy::Auto with CommitAfter::Marked, closes the "
                   "reader and reads back the offset of the mark. A reader without Auto refuses the mark.");
    auto client         = GetLoggedInHighLevelClient();
    const auto stream   = CreateTopicWithMessages(client, kMessagesCount);
    const auto topic    = iggy::Identifier::Numeric(0);
    const auto consumer = iggy::Consumer::Single(iggy::Identifier::Numeric(1));
    auto options        = standalone_options(0, iggy::CommitPolicy::Auto);
    options.SetPollOptions(iggy::PollOptions()
                               .SetCount(kBatchCount)
                               .SetAutoCommit(iggy::AutoCommit()
                                                  .SetInterval(std::nullopt)
                                                  .SetEvery(iggy::CommitEvery::Batch())
                                                  .SetAfter(iggy::CommitAfter::Marked)));
    auto reader = client.Reader(consumer, stream, topic, options);

    const auto batch = reader.Next(kReadTimeout);
    ASSERT_TRUE(batch.has_value());
    ASSERT_GE(batch->Messages().size(), 2u);
    ASSERT_NO_THROW(reader.Mark(batch->CheckpointAt(1)));
    ASSERT_NO_THROW(reader.Close());

    const auto stored = client.GetConsumerOffset(consumer, stream, topic, 0);
    EXPECT_EQ(stored.StoredOffset(), 1u);

    auto manual             = client.Reader(iggy::Consumer::Single(iggy::Identifier::Numeric(2)), stream, topic,
                                            standalone_options(0, iggy::CommitPolicy::Manual));
    const auto manual_batch = manual.Next(kReadTimeout);
    ASSERT_TRUE(manual_batch.has_value());
    ASSERT_FALSE(manual_batch->Messages().empty());
    try {
        manual.Mark(manual_batch->Checkpoint());
        FAIL() << "A manual commit policy must refuse a mark";
    } catch (const iggy::IggyException &error) {
        EXPECT_EQ(error.Kind(), "invalid_command");
    }
    ASSERT_NO_THROW(manual.Close());
}

TEST_F(E2E_Reader, OneShotPollRefusesAutoCommit) {
    RecordProperty("description", "A one-shot poll has nothing to count, so CommitPolicy::Auto is refused.");
    auto client       = GetLoggedInHighLevelClient();
    const auto stream = CreateTopicWithMessages(client, 1);

    try {
        client.PollMessages(iggy::Consumer::Single(iggy::Identifier::Numeric(1)), stream, iggy::Identifier::Numeric(0),
                            0, iggy::PollingStrategy::Offset(0),
                            iggy::PollOptions().SetCount(1).SetCommit(iggy::CommitPolicy::Auto));
        FAIL() << "A one-shot poll must refuse automatic commits";
    } catch (const iggy::IggyException &error) {
        EXPECT_EQ(error.Kind(), "invalid_configuration");
    }
}
