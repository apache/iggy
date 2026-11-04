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

#include <cstddef>
#include <cstdint>
#include <string>

#include <gtest/gtest.h>

#include "iggy.hpp"

TEST(MessageTest, CreateSetsPayload) {
    RecordProperty("description", "Verifies Create stores payload bytes correctly.");
    const std::string text = "hello world";
    const auto message     = iggy::IggyMessageToSend::Create(text);

    ASSERT_EQ(message.Payload().size(), text.size());
    for (std::size_t index = 0; index < text.size(); ++index) {
        EXPECT_EQ(message.Payload()[index], static_cast<std::uint8_t>(text[index]));
    }
}

TEST(MessageTest, CreateDefaultsIdAndHeaders) {
    RecordProperty("description", "Verifies Create initializes the message ID to zero and headers to empty.");
    const auto message = iggy::IggyMessageToSend::Create("B");

    EXPECT_EQ(message.Id(), 0);
    EXPECT_TRUE(message.UserHeaders().empty());
}

TEST(MessageTest, CreateWithEmptyPayload) {
    RecordProperty("description", "Verifies Create accepts an empty payload before send-time validation.");
    const auto message = iggy::IggyMessageToSend::Create("");

    EXPECT_TRUE(message.Payload().empty());
}

TEST(MessageTest, CreateWithSingleByte) {
    RecordProperty("description", "Verifies Create works with a single-byte payload.");
    const std::string payload(1, static_cast<char>(0xFF));
    const auto message = iggy::IggyMessageToSend::Create(payload);

    ASSERT_EQ(message.Payload().size(), 1u);
    EXPECT_EQ(message.Payload()[0], 0xFF);
}

TEST(MessageTest, CreatePreservesNullBytes) {
    RecordProperty("description", "Verifies Create preserves null bytes in the payload.");
    std::string payload(3, '\0');
    payload[1]         = '\1';
    const auto message = iggy::IggyMessageToSend::Create(payload);

    ASSERT_EQ(message.Payload().size(), 3u);
    EXPECT_EQ(message.Payload()[0], 0x00);
    EXPECT_EQ(message.Payload()[1], 0x01);
    EXPECT_EQ(message.Payload()[2], 0x00);
}

TEST(MessageTest, CreateWithCustomIdPreservesPayload) {
    RecordProperty("description", "Verifies Create keeps a full-width custom ID without affecting the payload.");
    const auto message = iggy::IggyMessageToSend::Create("B", {}, absl::MakeUint128(200, 100));

    EXPECT_EQ(absl::Uint128Low64(message.Id()), 100u);
    EXPECT_EQ(absl::Uint128High64(message.Id()), 200u);
    ASSERT_EQ(message.Payload().size(), 1u);
    EXPECT_EQ(message.Payload()[0], 0x42);
}

TEST(MessageTest, CreateWithLargePayload) {
    RecordProperty("description", "Verifies Create handles a larger binary payload correctly.");
    std::string payload(10000, '\0');
    for (std::size_t index = 0; index < payload.size(); ++index) {
        payload[index] = static_cast<char>(index % 256);
    }
    const auto message = iggy::IggyMessageToSend::Create(payload);

    ASSERT_EQ(message.Payload().size(), 10000u);
    EXPECT_EQ(message.Payload()[0], 0u);
    EXPECT_EQ(message.Payload()[255], 255u);
    EXPECT_EQ(message.Payload()[256], 0u);
}
