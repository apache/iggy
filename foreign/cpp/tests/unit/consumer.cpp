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

#include <gtest/gtest.h>

#include "iggy.hpp"

namespace {

std::string identifier_text(const iggy::ffi::Identifier &identifier) {
    return std::string(identifier.value.begin(), identifier.value.end());
}

}  // namespace

TEST(ConsumerTest, SingleFromNameCarriesConsumerKind) {
    const auto consumer = iggy::Consumer::Single("order-processor");

    EXPECT_EQ(consumer.kind, iggy::ffi::ConsumerKind::Consumer);
    EXPECT_EQ(consumer.id.kind, "string");
    EXPECT_EQ(identifier_text(consumer.id), "order-processor");
}

TEST(ConsumerTest, SingleFromNumberCarriesConsumerKind) {
    const auto consumer = iggy::Consumer::Single(7);

    EXPECT_EQ(consumer.kind, iggy::ffi::ConsumerKind::Consumer);
    EXPECT_EQ(consumer.id.kind, "numeric");
    EXPECT_EQ(consumer.id.length, 4u);
}

TEST(ConsumerTest, GroupFromNameCarriesConsumerGroupKind) {
    const auto consumer = iggy::Consumer::Group("order-processors");

    EXPECT_EQ(consumer.kind, iggy::ffi::ConsumerKind::ConsumerGroup);
    EXPECT_EQ(consumer.id.kind, "string");
    EXPECT_EQ(identifier_text(consumer.id), "order-processors");
}

TEST(ConsumerTest, GroupFromNumberCarriesConsumerGroupKind) {
    const auto consumer = iggy::Consumer::Group(7);

    EXPECT_EQ(consumer.kind, iggy::ffi::ConsumerKind::ConsumerGroup);
    EXPECT_EQ(consumer.id.kind, "numeric");
    EXPECT_EQ(consumer.id.length, 4u);
}

TEST(ConsumerTest, RejectsEmptyName) {
    EXPECT_THROW(iggy::Consumer::Single(""), iggy::IggyException);
    EXPECT_THROW(iggy::Consumer::Group(""), iggy::IggyException);
}

TEST(ConsumerTest, RejectsNameLongerThan255Bytes) {
    const std::string too_long_name(256, 'a');

    EXPECT_THROW(iggy::Consumer::Single(too_long_name), iggy::IggyException);
    EXPECT_THROW(iggy::Consumer::Group(too_long_name), iggy::IggyException);
}

TEST(AnyPartitionIdTest, LeavesThePartitionToTheServer) {
    EXPECT_EQ(iggy::kAnyPartitionId, std::numeric_limits<std::uint32_t>::max());
}
