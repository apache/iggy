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

#include <string>

#include <gtest/gtest.h>

#include "iggy.hpp"

TEST(CompressionAlgorithmTest, ReturnsExpectedValues) {
    EXPECT_EQ(iggy::CompressionAlgorithm::none().algorithm(), "none");
    EXPECT_EQ(iggy::CompressionAlgorithm::gzip().algorithm(), "gzip");
}

TEST(MaxTopicSizeTest, ReturnsExpectedValues) {
    EXPECT_EQ(iggy::MaxTopicSize::server_default().max_topic_size(), "server_default");
    EXPECT_EQ(iggy::MaxTopicSize::unlimited().max_topic_size(), "unlimited");
    EXPECT_EQ(iggy::MaxTopicSize::from_bytes(1024).max_topic_size(), "1024");
}

TEST(PollingStrategyTest, ReturnsExpectedKindAndValue) {
    const auto offset = iggy::PollingStrategy::offset(7);
    EXPECT_EQ(offset.polling_strategy_kind(), "offset");
    EXPECT_EQ(offset.polling_strategy_value(), 7u);

    const auto timestamp = iggy::PollingStrategy::timestamp(42);
    EXPECT_EQ(timestamp.polling_strategy_kind(), "timestamp");
    EXPECT_EQ(timestamp.polling_strategy_value(), 42u);

    const auto first = iggy::PollingStrategy::first();
    EXPECT_EQ(first.polling_strategy_kind(), "first");
    EXPECT_EQ(first.polling_strategy_value(), 0u);

    const auto last = iggy::PollingStrategy::last();
    EXPECT_EQ(last.polling_strategy_kind(), "last");
    EXPECT_EQ(last.polling_strategy_value(), 0u);

    const auto next = iggy::PollingStrategy::next();
    EXPECT_EQ(next.polling_strategy_kind(), "next");
    EXPECT_EQ(next.polling_strategy_value(), 0u);
}

TEST(ExpiryTest, CanCreateAllVariants) {
    (void)iggy::Expiry::server_default();
    (void)iggy::Expiry::never_expire();
    (void)iggy::Expiry::duration(15);
    SUCCEED();
}

TEST(IggyExceptionTest, StoresMessage) {
    const iggy::IggyException from_cstr("boom");
    EXPECT_EQ(std::string(from_cstr.what()), "boom");

    const std::string message = "boom2";
    const iggy::IggyException from_string(message);
    EXPECT_EQ(std::string(from_string.what()), message);
}
