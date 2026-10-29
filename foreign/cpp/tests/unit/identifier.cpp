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

TEST(IdentifierTest, FromStringCreatesStringIdentifier) {
    RecordProperty("description", "Creates a string identifier and preserves its payload.");
    const std::string value = "stream-identifier";

    iggy::Identifier identifier = iggy::Identifier::String(value);

    EXPECT_EQ(identifier.Type(), iggy::Identifier::Kind::String);
    ASSERT_TRUE(std::holds_alternative<std::string>(identifier.Value()));
    EXPECT_EQ(std::get<std::string>(identifier.Value()), value);
}

TEST(IdentifierTest, FromStringAcceptsExact255ByteUtf8Value) {
    RecordProperty("description", "Accepts a UTF-8 string identifier whose encoded byte length is exactly 255.");
    std::string value;
    for (size_t i = 0; i < 127; ++i) {
        value += "\xC2\xA2";
    }
    value += "a";

    ASSERT_EQ(value.size(), 255u);

    iggy::Identifier identifier = iggy::Identifier::String(value);

    EXPECT_EQ(identifier.Type(), iggy::Identifier::Kind::String);
    ASSERT_TRUE(std::holds_alternative<std::string>(identifier.Value()));
    EXPECT_EQ(std::get<std::string>(identifier.Value()), value);
}

TEST(IdentifierTest, FromStringRejectsEmptyValue) {
    RecordProperty("description", "Rejects creating a string identifier from an empty string.");
    EXPECT_THROW(iggy::Identifier::String(""), iggy::IggyException);
}

TEST(IdentifierTest, FromStringRejectsUtf8ValueLongerThan255Bytes) {
    RecordProperty("description", "Rejects creating a UTF-8 string identifier longer than 255 encoded bytes.");
    std::string too_long_value;
    for (size_t i = 0; i < 128; ++i) {
        too_long_value += "\xC2\xA2";
    }

    ASSERT_EQ(too_long_value.size(), 256u);

    EXPECT_THROW(iggy::Identifier::String(too_long_value), iggy::IggyException);
}

TEST(IdentifierTest, FromStringRejectsAsciiValueLongerThan255Bytes) {
    RecordProperty("description", "Rejects creating an ASCII string identifier longer than 255 bytes.");
    const std::string too_long_value(256, 'a');

    EXPECT_THROW(iggy::Identifier::String(too_long_value), iggy::IggyException);
}

TEST(IdentifierTest, FromNumericCreatesNumericIdentifier) {
    RecordProperty("description", "Creates a numeric identifier preserving its value.");
    constexpr std::uint32_t value = 0x12345678;

    iggy::Identifier identifier = iggy::Identifier::Numeric(value);

    EXPECT_EQ(identifier.Type(), iggy::Identifier::Kind::Numeric);
    ASSERT_TRUE(std::holds_alternative<std::uint32_t>(identifier.Value()));
    EXPECT_EQ(std::get<std::uint32_t>(identifier.Value()), value);
}

TEST(IdentifierTest, FromNumericCreatesUint32MaxIdentifier) {
    RecordProperty("description", "Creates a numeric identifier for UINT32_MAX.");
    constexpr std::uint32_t value = std::numeric_limits<std::uint32_t>::max();

    iggy::Identifier identifier = iggy::Identifier::Numeric(value);

    EXPECT_EQ(identifier.Type(), iggy::Identifier::Kind::Numeric);
    ASSERT_TRUE(std::holds_alternative<std::uint32_t>(identifier.Value()));
    EXPECT_EQ(std::get<std::uint32_t>(identifier.Value()), value);
}
