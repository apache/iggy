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

TEST(MessageTest, NewMessageSetsPayloadAndLength) {
    RecordProperty("description", "Verifies new_message sets payload and payload_length correctly.");
    iggy::ffi::Message msg;
    rust::Vec<std::uint8_t> payload;
    const std::string text = "hello world";
    for (const char c : text) {
        payload.push_back(static_cast<std::uint8_t>(c));
    }

    msg.new_message(std::move(payload));

    ASSERT_EQ(msg.payload_length, static_cast<std::uint32_t>(text.size()));
    ASSERT_EQ(msg.payload.size(), text.size());
    for (std::size_t i = 0; i < text.size(); i++) {
        EXPECT_EQ(msg.payload[i], static_cast<std::uint8_t>(text[i]));
    }
}

TEST(MessageTest, NewMessageZerosHeaderFields) {
    RecordProperty("description", "Verifies new_message initializes all header fields to zero.");
    iggy::ffi::Message msg;
    msg.checksum         = 999;
    msg.id_lo            = 999;
    msg.id_hi            = 999;
    msg.offset           = 999;
    msg.timestamp        = 999;
    msg.origin_timestamp = 999;
    msg.reserved         = 999;

    rust::Vec<std::uint8_t> payload;
    payload.push_back(0x42);
    msg.new_message(std::move(payload));

    EXPECT_EQ(msg.checksum, 0u);
    EXPECT_EQ(msg.id_lo, 0u);
    EXPECT_EQ(msg.id_hi, 0u);
    EXPECT_EQ(msg.offset, 0u);
    EXPECT_EQ(msg.timestamp, 0u);
    EXPECT_EQ(msg.origin_timestamp, 0u);
    EXPECT_EQ(msg.user_headers_length, 0u);
    EXPECT_EQ(msg.reserved, 0u);
    EXPECT_TRUE(msg.user_headers.empty());
}

TEST(MessageTest, NewMessageWithEmptyPayload) {
    RecordProperty("description", "Verifies new_message accepts an empty payload.");
    iggy::ffi::Message msg;
    rust::Vec<std::uint8_t> empty_payload;

    msg.new_message(std::move(empty_payload));

    ASSERT_EQ(msg.payload_length, 0u);
    ASSERT_EQ(msg.payload.size(), 0u);
}

TEST(MessageTest, NewMessageWithSingleByte) {
    RecordProperty("description", "Verifies new_message works with a single-byte payload.");
    iggy::ffi::Message msg;
    rust::Vec<std::uint8_t> payload;
    payload.push_back(0xFF);

    msg.new_message(std::move(payload));

    ASSERT_EQ(msg.payload_length, 1u);
    ASSERT_EQ(msg.payload.size(), 1u);
    EXPECT_EQ(msg.payload[0], 0xFF);
}

TEST(MessageTest, NewMessageWithNullBytes) {
    RecordProperty("description", "Verifies new_message preserves null bytes in payload.");
    iggy::ffi::Message msg;
    rust::Vec<std::uint8_t> payload;
    payload.push_back(0x00);
    payload.push_back(0x01);
    payload.push_back(0x00);

    msg.new_message(std::move(payload));

    ASSERT_EQ(msg.payload_length, 3u);
    ASSERT_EQ(msg.payload.size(), 3u);
    EXPECT_EQ(msg.payload[0], 0x00);
    EXPECT_EQ(msg.payload[1], 0x01);
    EXPECT_EQ(msg.payload[2], 0x00);
}

TEST(MessageTest, NewMessageOverwritesPreviousState) {
    RecordProperty("description", "Verifies calling new_message twice overwrites previous payload and fields.");
    iggy::ffi::Message msg;

    rust::Vec<std::uint8_t> payload1;
    payload1.push_back(0xAA);
    payload1.push_back(0xBB);
    msg.new_message(std::move(payload1));
    msg.id_lo = 42;

    rust::Vec<std::uint8_t> payload2;
    payload2.push_back(0xCC);
    msg.new_message(std::move(payload2));

    ASSERT_EQ(msg.payload_length, 1u);
    ASSERT_EQ(msg.payload.size(), 1u);
    EXPECT_EQ(msg.payload[0], 0xCC);
    EXPECT_EQ(msg.id_lo, 0u);
}

TEST(MessageTest, NewMessageThenSetCustomId) {
    RecordProperty("description", "Verifies custom ID can be set after new_message without affecting payload.");
    iggy::ffi::Message msg;
    rust::Vec<std::uint8_t> payload;
    payload.push_back(0x42);
    msg.new_message(std::move(payload));

    msg.id_lo = 100;
    msg.id_hi = 200;

    EXPECT_EQ(msg.id_lo, 100u);
    EXPECT_EQ(msg.id_hi, 200u);
    ASSERT_EQ(msg.payload_length, 1u);
    EXPECT_EQ(msg.payload[0], 0x42);
}

TEST(MessageTest, NewMessageWithLargePayload) {
    RecordProperty("description", "Verifies new_message handles a larger payload correctly.");
    iggy::ffi::Message msg;
    rust::Vec<std::uint8_t> payload;
    for (std::uint32_t i = 0; i < 10000; i++) {
        payload.push_back(static_cast<std::uint8_t>(i % 256));
    }

    msg.new_message(std::move(payload));

    ASSERT_EQ(msg.payload_length, 10000u);
    ASSERT_EQ(msg.payload.size(), 10000u);
    EXPECT_EQ(msg.payload[0], 0u);
    EXPECT_EQ(msg.payload[255], 255u);
    EXPECT_EQ(msg.payload[256], 0u);
}
