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
#include <optional>

#include <gtest/gtest.h>

#include "iggy.hpp"

TEST(PollOptionsTest, DefaultOptionsReadAtOnceAndStoreNothing) {
    const iggy::PollOptions options;

    EXPECT_EQ(options.Count(), 1000u);
    EXPECT_EQ(options.MaxWait(), std::chrono::microseconds(0));
    EXPECT_EQ(options.MinCount(), 1u);
    EXPECT_EQ(options.MaxBytes(), 1024u * 1024u);
    EXPECT_EQ(options.Commit(), iggy::CommitPolicy::Manual);
}

TEST(PollOptionsTest, DefaultAutoCommitStoresEveryTenThousandMessagesOrFiveSecondsAfterProcessing) {
    const iggy::AutoCommit auto_commit;

    EXPECT_EQ(auto_commit.Interval(), std::optional(std::chrono::microseconds(std::chrono::seconds(5))));
    ASSERT_TRUE(auto_commit.Every().has_value());
    EXPECT_EQ(auto_commit.Every()->GetKind(), iggy::CommitEvery::Kind::Messages);
    EXPECT_EQ(auto_commit.Every()->Count(), 10'000u);
    EXPECT_EQ(auto_commit.After(), iggy::CommitAfter::Processing);
}

TEST(PollOptionsTest, SetAutoCommitSwitchesToAutoAndKeepsItsSettings) {
    iggy::PollOptions options;
    options.SetCount(7).SetMaxWait(std::chrono::milliseconds(250)).SetMinCount(2).SetMaxBytes(4096);
    options.SetAutoCommit(iggy::AutoCommit()
                              .SetInterval(std::nullopt)
                              .SetEvery(iggy::CommitEvery::Batch())
                              .SetAfter(iggy::CommitAfter::Marked));

    EXPECT_EQ(options.Count(), 7u);
    EXPECT_EQ(options.MaxWait(), std::chrono::microseconds(250'000));
    EXPECT_EQ(options.MinCount(), 2u);
    EXPECT_EQ(options.MaxBytes(), 4096u);
    EXPECT_EQ(options.Commit(), iggy::CommitPolicy::Auto);
    const auto auto_commit = options.AutoCommitSettings();
    EXPECT_FALSE(auto_commit.Interval().has_value());
    ASSERT_TRUE(auto_commit.Every().has_value());
    EXPECT_EQ(auto_commit.Every()->GetKind(), iggy::CommitEvery::Kind::Batch);
    EXPECT_EQ(auto_commit.After(), iggy::CommitAfter::Marked);
}

TEST(PollOptionsTest, NegativeWaitAnswersAtOnce) {
    iggy::PollOptions options;
    options.SetMaxWait(std::chrono::microseconds(-1));

    EXPECT_EQ(options.MaxWait(), std::chrono::microseconds(0));
}

TEST(PollOptionsTest, ConsumerAcceptsAContinuation) {
    using SeekContinuation      = void (iggy::IggyConsumer::*)(std::uint32_t, const iggy::Continuation &);
    const SeekContinuation seek = &iggy::IggyConsumer::Seek;

    EXPECT_NE(seek, nullptr);
}
