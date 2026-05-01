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

#include <cstdint>
#include <string>

#include <gtest/gtest.h>

#include "lib.rs.h"

TEST(MessageTest, MakeMessageSetsPayload) {
    RecordProperty("description", "Verifies make_message stores payload bytes correctly.");
    rust::Vec<std::uint8_t> payload;
    const std::string text = "hello world";
    for (const char c : text) {
        payload.push_back(static_cast<std::uint8_t>(c));
    }

    auto msg = iggy::ffi::make_message(std::move(payload));

    ASSERT_EQ(msg.payload.size(), text.size());
    for (std::size_t i = 0; i < text.size(); i++) {
        EXPECT_EQ(msg.payload[i], static_cast<std::uint8_t>(text[i]));
    }
}

TEST(MessageTest, MakeMessageZerosIdAndHeaders) {
    RecordProperty("description", "Verifies make_message initializes id and user_headers to zero/empty.");
    rust::Vec<std::uint8_t> payload;
    payload.push_back(0x42);

    auto msg = iggy::ffi::make_message(std::move(payload));

    EXPECT_EQ(msg.id_lo, 0u);
    EXPECT_EQ(msg.id_hi, 0u);
    EXPECT_TRUE(msg.user_headers.empty());
}

TEST(MessageTest, MakeMessageWithEmptyPayload) {
    RecordProperty("description", "Verifies make_message accepts an empty payload.");
    rust::Vec<std::uint8_t> empty_payload;

    auto msg = iggy::ffi::make_message(std::move(empty_payload));

    ASSERT_EQ(msg.payload.size(), 0u);
}

TEST(MessageTest, MakeMessageWithSingleByte) {
    RecordProperty("description", "Verifies make_message works with a single-byte payload.");
    rust::Vec<std::uint8_t> payload;
    payload.push_back(0xFF);

    auto msg = iggy::ffi::make_message(std::move(payload));

    ASSERT_EQ(msg.payload.size(), 1u);
    EXPECT_EQ(msg.payload[0], 0xFF);
}

TEST(MessageTest, MakeMessageWithNullBytes) {
    RecordProperty("description", "Verifies make_message preserves null bytes in payload.");
    rust::Vec<std::uint8_t> payload;
    payload.push_back(0x00);
    payload.push_back(0x01);
    payload.push_back(0x00);

    auto msg = iggy::ffi::make_message(std::move(payload));

    ASSERT_EQ(msg.payload.size(), 3u);
    EXPECT_EQ(msg.payload[0], 0x00);
    EXPECT_EQ(msg.payload[1], 0x01);
    EXPECT_EQ(msg.payload[2], 0x00);
}

TEST(MessageTest, MakeMessageThenSetCustomId) {
    RecordProperty("description", "Verifies custom ID can be set after make_message without affecting payload.");
    rust::Vec<std::uint8_t> payload;
    payload.push_back(0x42);
    auto msg = iggy::ffi::make_message(std::move(payload));

    msg.id_lo = 100;
    msg.id_hi = 200;

    EXPECT_EQ(msg.id_lo, 100u);
    EXPECT_EQ(msg.id_hi, 200u);
    ASSERT_EQ(msg.payload.size(), 1u);
    EXPECT_EQ(msg.payload[0], 0x42);
}

TEST(MessageTest, MakeMessageWithLargePayload) {
    RecordProperty("description", "Verifies make_message handles a larger payload correctly.");
    rust::Vec<std::uint8_t> payload;
    for (std::uint32_t i = 0; i < 10000; i++) {
        payload.push_back(static_cast<std::uint8_t>(i % 256));
    }

    auto msg = iggy::ffi::make_message(std::move(payload));

    ASSERT_EQ(msg.payload.size(), 10000u);
    EXPECT_EQ(msg.payload[0], 0u);
    EXPECT_EQ(msg.payload[255], 255u);
    EXPECT_EQ(msg.payload[256], 0u);
}
