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
#include <vector>

#include <gtest/gtest.h>

#include "lib.rs.h"
#include "tests/e2e/test_helpers.hpp"

namespace {

constexpr std::uint64_t kReadTimeoutMicros     = 10'000'000;
constexpr std::uint64_t kIdleReadTimeoutMicros = 200'000;
constexpr std::uint32_t kMessagesCount         = 10;
constexpr std::uint32_t kBatchCount            = 4;

iggy::ffi::ReaderOptions standalone_options(const std::uint32_t partition_id,
                                            const iggy::ffi::ServerCommitPolicy server_commit) {
    iggy::ffi::ReaderOptions options{};
    options.partitions.push_back(partition_id);
    options.has_start        = true;
    options.start            = make_polling_strategy("offset", 0);
    options.has_poll_options = true;
    options.poll_options     = make_poll_options(kBatchCount, server_commit);
    return options;
}

}  // namespace

class LowLevelE2E_Reader : public E2ETestFixture {
  protected:
    // One topic with one partition. The messages go to partition 0 and carry their index as payload.
    std::uint32_t CreateTopicWithMessages(iggy::ffi::Client *client, const std::uint32_t messages_count) {
        iggy::ffi::ErrorInfo error_info{};
        const std::string stream_name = GetRandomName();
        client->create_stream(stream_name, {}, error_info);
        const auto stream = client->get_stream(make_string_identifier(stream_name));
        TrackStream(stream.id);
        client->create_topic(make_numeric_identifier(stream.id), GetRandomName(), make_topic_create_options(1),
                             error_info);
        if (messages_count > 0) {
            rust::Vec<iggy::ffi::IggyMessageToSend> messages;
            for (std::uint32_t i = 0; i < messages_count; i++) {
                messages.push_back(
                    iggy::ffi::make_message(to_payload(std::to_string(i)), rust::Vec<iggy::ffi::HeaderEntry>()));
            }
            client->send_messages(make_numeric_identifier(stream.id), make_numeric_identifier(0), "partition_id",
                                  partition_id_bytes(0), std::move(messages), error_info);
        }
        return stream.id;
    }
};

TEST_F(LowLevelE2E_Reader, ReadsEveryMessageAndCommitStoresTheOffset) {
    RecordProperty("description",
                   "Reads every message of one partition with a reader, commits the last checkpoint and reads the "
                   "stored offset back.");
    iggy::ffi::ErrorInfo error_info{};
    iggy::ffi::ReadErrorInfo read_error{};
    iggy::ffi::Client *client     = GetLoggedInClient();
    const std::uint32_t stream_id = CreateTopicWithMessages(client, kMessagesCount);

    auto reader           = client->reader(make_numeric_identifier(stream_id), make_numeric_identifier(0), "consumer",
                                           make_numeric_identifier(1),
                                           standalone_options(0, iggy::ffi::ServerCommitPolicy::Disabled), error_info);
    const auto partitions = reader->partitions();
    ASSERT_EQ(partitions.size(), 1u);
    EXPECT_EQ(partitions[0], 0u);

    std::vector<std::uint64_t> offsets;
    std::optional<rust::Box<iggy::ffi::Checkpoint>> last;
    while (offsets.size() < kMessagesCount) {
        auto batch = reader->next(kReadTimeoutMicros, read_error);
        ASSERT_FALSE(batch.messages.empty()) << "No batch before the timeout after " << offsets.size() << " messages";
        EXPECT_EQ(batch.partition_id, 0u);
        for (const auto &message : batch.messages) {
            offsets.push_back(message.offset);
        }
        last = batch.checkpoint();
    }
    for (std::uint32_t i = 0; i < kMessagesCount; i++) {
        EXPECT_EQ(offsets[i], static_cast<std::uint64_t>(i)) << "Offset mismatch at index " << i;
    }
    ASSERT_TRUE(last.has_value());
    const auto &checkpoint = **last;
    EXPECT_EQ(checkpoint.partition_id(), 0u);
    EXPECT_EQ(checkpoint.offset(), kMessagesCount - 1);

    ASSERT_NO_THROW(reader->commit(checkpoint, error_info));
    const auto stored = client->get_consumer_offset(make_numeric_identifier(stream_id), make_numeric_identifier(0), 0,
                                                    "consumer", make_numeric_identifier(1));
    EXPECT_EQ(stored.stored_offset, kMessagesCount - 1);
    ASSERT_NO_THROW(reader->close(error_info));
}

TEST_F(LowLevelE2E_Reader, SeekReadsThePartitionAgainFromTheNewStart) {
    RecordProperty("description",
                   "Reads one batch, seeks back to offset 0 and expects the next batch to start there. Then reads on, "
                   "seeks to the continuation of the first batch and expects the next batch to start after it.");
    iggy::ffi::ErrorInfo error_info{};
    iggy::ffi::ReadErrorInfo read_error{};
    iggy::ffi::Client *client     = GetLoggedInClient();
    const std::uint32_t stream_id = CreateTopicWithMessages(client, kMessagesCount);
    auto reader = client->reader(make_numeric_identifier(stream_id), make_numeric_identifier(0), "consumer",
                                 make_numeric_identifier(1),
                                 standalone_options(0, iggy::ffi::ServerCommitPolicy::Disabled), error_info);

    const auto first = reader->next(kReadTimeoutMicros, read_error);
    ASSERT_FALSE(first.messages.empty());
    EXPECT_EQ(first.messages.front().offset, 0u);

    ASSERT_NO_THROW(reader->seek(0, make_polling_strategy("offset", 0), error_info));
    const auto again = reader->next(kReadTimeoutMicros, read_error);
    ASSERT_FALSE(again.messages.empty());
    EXPECT_EQ(again.messages.front().offset, 0u) << "A batch queued before the seek was delivered after it";

    const auto later = reader->next(kReadTimeoutMicros, read_error);
    ASSERT_FALSE(later.messages.empty());
    ASSERT_NO_THROW(reader->seek(0, *first.continuation(), error_info));
    const auto resumed = reader->next(kReadTimeoutMicros, read_error);
    ASSERT_FALSE(resumed.messages.empty());
    EXPECT_EQ(resumed.messages.front().offset, first.messages.back().offset + 1)
        << "A seek to a continuation must read on after its batch";

    EXPECT_THROW(reader->seek(1, make_polling_strategy("offset", 0), error_info), std::exception)
        << "A seek of a partition that the reader does not read must fail";
    EXPECT_EQ(error_info.kind, "invalid_command");
    ASSERT_NO_THROW(reader->close(error_info));
}

TEST_F(LowLevelE2E_Reader, NextReturnsAnEmptyBatchWhenTheTimeRunsOut) {
    RecordProperty("description", "Waits for the next batch of an empty partition and gets an empty batch.");
    iggy::ffi::ErrorInfo error_info{};
    iggy::ffi::ReadErrorInfo read_error{};
    iggy::ffi::Client *client     = GetLoggedInClient();
    const std::uint32_t stream_id = CreateTopicWithMessages(client, 0);
    auto reader = client->reader(make_numeric_identifier(stream_id), make_numeric_identifier(0), "consumer",
                                 make_numeric_identifier(1),
                                 standalone_options(0, iggy::ffi::ServerCommitPolicy::Disabled), error_info);

    const auto batch = reader->next(kIdleReadTimeoutMicros, read_error);
    EXPECT_TRUE(batch.messages.empty());
    EXPECT_EQ(batch.count, 0u);
    EXPECT_THROW(batch.checkpoint(), std::exception);
    ASSERT_NO_THROW(reader->close(error_info));
}

TEST_F(LowLevelE2E_Reader, CommitIsRefusedWhenTheServerCommitsOnRead) {
    RecordProperty("description", "A reader whose server commits on read refuses a commit from the caller.");
    iggy::ffi::ErrorInfo error_info{};
    iggy::ffi::ReadErrorInfo read_error{};
    iggy::ffi::Client *client     = GetLoggedInClient();
    const std::uint32_t stream_id = CreateTopicWithMessages(client, kMessagesCount);
    auto reader = client->reader(make_numeric_identifier(stream_id), make_numeric_identifier(0), "consumer",
                                 make_numeric_identifier(1),
                                 standalone_options(0, iggy::ffi::ServerCommitPolicy::OnRead), error_info);

    const auto batch = reader->next(kReadTimeoutMicros, read_error);
    ASSERT_FALSE(batch.messages.empty());
    EXPECT_THROW(reader->commit(*batch.checkpoint(), error_info), std::exception);
    EXPECT_EQ(error_info.kind, "invalid_command");
    ASSERT_NO_THROW(reader->close(error_info));
}

TEST_F(LowLevelE2E_Reader, EveryCallButCloseThrowsAfterClose) {
    RecordProperty("description", "A closed reader refuses every later call, and a second close does nothing.");
    iggy::ffi::ErrorInfo error_info{};
    iggy::ffi::ReadErrorInfo read_error{};
    iggy::ffi::Client *client     = GetLoggedInClient();
    const std::uint32_t stream_id = CreateTopicWithMessages(client, kMessagesCount);
    auto reader      = client->reader(make_numeric_identifier(stream_id), make_numeric_identifier(0), "consumer",
                                      make_numeric_identifier(1),
                                      standalone_options(0, iggy::ffi::ServerCommitPolicy::Disabled), error_info);
    const auto batch = reader->next(kReadTimeoutMicros, read_error);
    ASSERT_FALSE(batch.messages.empty());

    ASSERT_NO_THROW(reader->close(error_info));
    EXPECT_THROW(reader->next(kIdleReadTimeoutMicros, read_error), std::exception);
    EXPECT_THROW(reader->commit(*batch.checkpoint(), error_info), std::exception);
    EXPECT_THROW(reader->seek(0, make_polling_strategy("offset", 0), error_info), std::exception);
    EXPECT_THROW(reader->seek(0, *batch.continuation(), error_info), std::exception);
    EXPECT_THROW(reader->partitions(), std::exception);
    EXPECT_NO_THROW(reader->close(error_info));
}

TEST_F(LowLevelE2E_Reader, ConsumerGroupReaderJoinsInBuildAndLeavesInClose) {
    RecordProperty("description",
                   "A group reader joins its group when it is built, reads the assigned partition and leaves the "
                   "group when it is closed.");
    iggy::ffi::ErrorInfo error_info{};
    iggy::ffi::ReadErrorInfo read_error{};
    iggy::ffi::Client *client     = GetLoggedInClient();
    const std::uint32_t stream_id = CreateTopicWithMessages(client, kMessagesCount);
    const auto group = client->create_consumer_group(make_numeric_identifier(stream_id), make_numeric_identifier(0),
                                                     GetRandomName(), error_info);

    iggy::ffi::ReaderOptions options{};
    options.has_start = true;
    options.start     = make_polling_strategy("offset", 0);
    auto reader       = client->reader(make_numeric_identifier(stream_id), make_numeric_identifier(0), "consumer_group",
                                       make_numeric_identifier(group.id), std::move(options), error_info);
    EXPECT_EQ(client
                  ->get_consumer_group(make_numeric_identifier(stream_id), make_numeric_identifier(0),
                                       make_numeric_identifier(group.id))
                  .members_count,
              1u);

    const auto batch = reader->next(kReadTimeoutMicros, read_error);
    ASSERT_FALSE(batch.messages.empty());
    EXPECT_EQ(batch.partition_id, 0u);
    EXPECT_EQ(batch.messages.front().offset, 0u);

    ASSERT_NO_THROW(reader->close(error_info));
    EXPECT_EQ(client
                  ->get_consumer_group(make_numeric_identifier(stream_id), make_numeric_identifier(0),
                                       make_numeric_identifier(group.id))
                  .members_count,
              0u);
}
