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
#include <limits>
#include <string>
#include <variant>

#include <gtest/gtest.h>

#include "iggy.hpp"

TEST(ConsumerTest, SingleFromNameCarriesConsumerKind) {
    const auto consumer = iggy::Consumer::Single(iggy::Identifier::String("order-processor"));

    EXPECT_EQ(consumer.Type(), iggy::Consumer::Kind::Single);
    EXPECT_EQ(consumer.Id().Type(), iggy::Identifier::Kind::String);
    EXPECT_EQ(std::get<std::string>(consumer.Id().Value()), "order-processor");
}

TEST(ConsumerTest, SingleFromNumberCarriesConsumerKind) {
    const auto consumer = iggy::Consumer::Single(iggy::Identifier::Numeric(7));

    EXPECT_EQ(consumer.Type(), iggy::Consumer::Kind::Single);
    EXPECT_EQ(consumer.Id().Type(), iggy::Identifier::Kind::Numeric);
    EXPECT_EQ(std::get<std::uint32_t>(consumer.Id().Value()), 7u);
}

TEST(ConsumerTest, GroupFromNameCarriesConsumerGroupKind) {
    const auto consumer = iggy::Consumer::Group(iggy::Identifier::String("order-processors"));

    EXPECT_EQ(consumer.Type(), iggy::Consumer::Kind::Group);
    EXPECT_EQ(consumer.Id().Type(), iggy::Identifier::Kind::String);
    EXPECT_EQ(std::get<std::string>(consumer.Id().Value()), "order-processors");
}

TEST(ConsumerTest, GroupFromNumberCarriesConsumerGroupKind) {
    const auto consumer = iggy::Consumer::Group(iggy::Identifier::Numeric(7));

    EXPECT_EQ(consumer.Type(), iggy::Consumer::Kind::Group);
    EXPECT_EQ(consumer.Id().Type(), iggy::Identifier::Kind::Numeric);
    EXPECT_EQ(std::get<std::uint32_t>(consumer.Id().Value()), 7u);
}

TEST(ConsumerTest, RejectsEmptyName) {
    EXPECT_THROW((void)iggy::Consumer::Single(iggy::Identifier::String("")), iggy::IggyException);
    EXPECT_THROW((void)iggy::Consumer::Group(iggy::Identifier::String("")), iggy::IggyException);
}

TEST(ConsumerTest, RejectsNameLongerThan255Bytes) {
    const std::string too_long_name(256, 'a');

    EXPECT_THROW((void)iggy::Consumer::Single(iggy::Identifier::String(too_long_name)), iggy::IggyException);
    EXPECT_THROW((void)iggy::Consumer::Group(iggy::Identifier::String(too_long_name)), iggy::IggyException);
}

TEST(AnyPartitionIdTest, LeavesThePartitionToTheServer) {
    EXPECT_EQ(iggy::kAnyPartitionId, std::numeric_limits<std::uint32_t>::max());
}
