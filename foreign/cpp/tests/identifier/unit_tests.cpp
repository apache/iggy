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

#include <array>
#include <cstdint>
#include <limits>
#include <string>

#include <gtest/gtest.h>

#include "lib.rs.h"

TEST(LowLevelE2E_Identifier, FromStringCreatesStringIdentifier) {
    RecordProperty("description", "Creates a string identifier and preserves its UTF-8 byte payload.");
    const std::string value = "stream-identifier";
    iggy::ffi::Identifier identifier;

    ASSERT_NO_THROW(identifier.from_string(value));

    ASSERT_EQ(identifier.kind, "string");
    ASSERT_EQ(identifier.length, value.size());
    ASSERT_EQ(identifier.value.size(), value.size());
    for (size_t i = 0; i < value.size(); ++i) {
        EXPECT_EQ(identifier.value[i], static_cast<std::uint8_t>(value[i]));
    }
}

TEST(LowLevelE2E_Identifier, FromStringAcceptsExact255ByteUtf8Value) {
    RecordProperty("description", "Accepts a UTF-8 string identifier whose encoded byte length is exactly 255.");
    std::string value;
    for (size_t i = 0; i < 127; ++i) {
        value += "\xC2\xA2";
    }
    value += "a";

    ASSERT_EQ(value.size(), 255u);

    iggy::ffi::Identifier identifier;
    ASSERT_NO_THROW(identifier.from_string(value));

    ASSERT_EQ(identifier.kind, "string");
    ASSERT_EQ(identifier.length, value.size());
    ASSERT_EQ(identifier.value.size(), value.size());
    for (size_t i = 0; i < value.size(); ++i) {
        EXPECT_EQ(identifier.value[i], static_cast<std::uint8_t>(value[i]));
    }
}

TEST(LowLevelE2E_Identifier, FromStringRejectsEmptyValue) {
    RecordProperty("description", "Rejects creating a string identifier from an empty string.");
    iggy::ffi::Identifier identifier;

    ASSERT_THROW(identifier.from_string(""), std::exception);
}

TEST(LowLevelE2E_Identifier, FromStringRejectsUtf8ValueLongerThan255Bytes) {
    RecordProperty("description", "Rejects creating a UTF-8 string identifier longer than 255 encoded bytes.");
    iggy::ffi::Identifier identifier;
    std::string too_long_value;
    for (size_t i = 0; i < 128; ++i) {
        too_long_value += "\xC2\xA2";
    }

    ASSERT_EQ(too_long_value.size(), 256u);

    ASSERT_THROW(identifier.from_string(too_long_value), std::exception);
}

TEST(LowLevelE2E_Identifier, FromStringRejectsAsciiValueLongerThan255Bytes) {
    RecordProperty("description", "Rejects creating an ASCII string identifier longer than 255 bytes.");
    iggy::ffi::Identifier identifier;
    const std::string too_long_value(256, 'a');

    ASSERT_THROW(identifier.from_string(too_long_value), std::exception);
}

TEST(LowLevelE2E_Identifier, FromNumericCreatesNumericIdentifier) {
    RecordProperty("description", "Creates a numeric identifier encoded as four little-endian bytes.");
    iggy::ffi::Identifier identifier;
    constexpr std::uint32_t value                        = 0x12345678;
    constexpr std::array<std::uint8_t, 4> expected_bytes = {0x78, 0x56, 0x34, 0x12};

    ASSERT_NO_THROW(identifier.from_numeric(value));

    ASSERT_EQ(identifier.kind, "numeric");
    ASSERT_EQ(identifier.length, 4u);
    ASSERT_EQ(identifier.value.size(), expected_bytes.size());
    for (size_t i = 0; i < expected_bytes.size(); ++i) {
        EXPECT_EQ(identifier.value[i], expected_bytes[i]);
    }
}

TEST(LowLevelE2E_Identifier, FromNumericCreatesUint32MaxIdentifier) {
    RecordProperty("description", "Creates a numeric identifier for UINT32_MAX using four 0xFF bytes.");
    iggy::ffi::Identifier identifier;
    constexpr std::array<std::uint8_t, 4> expected_bytes = {0xFF, 0xFF, 0xFF, 0xFF};

    ASSERT_NO_THROW(identifier.from_numeric(std::numeric_limits<std::uint32_t>::max()));

    ASSERT_EQ(identifier.kind, "numeric");
    ASSERT_EQ(identifier.length, 4u);
    ASSERT_EQ(identifier.value.size(), expected_bytes.size());
    for (size_t i = 0; i < expected_bytes.size(); ++i) {
        EXPECT_EQ(identifier.value[i], expected_bytes[i]);
    }
}

TEST(LowLevelE2E_Identifier, FromNumericOverwritesExistingStringIdentifier) {
    RecordProperty("description", "Replaces a previously created string identifier with numeric identifier data.");
    iggy::ffi::Identifier identifier;

    ASSERT_NO_THROW(identifier.from_string("temporary-name"));
    ASSERT_NO_THROW(identifier.from_numeric(7));

    ASSERT_EQ(identifier.kind, "numeric");
    ASSERT_EQ(identifier.length, 4u);
    ASSERT_EQ(identifier.value.size(), 4u);
    EXPECT_EQ(identifier.value[0], 7u);
    EXPECT_EQ(identifier.value[1], 0u);
    EXPECT_EQ(identifier.value[2], 0u);
    EXPECT_EQ(identifier.value[3], 0u);
}

TEST(LowLevelE2E_Identifier, FromStringOverwritesExistingNumericIdentifier) {
    RecordProperty("description", "Replaces a previously created numeric identifier with string identifier data.");
    const std::string value = "replacement-name";
    iggy::ffi::Identifier identifier;

    ASSERT_NO_THROW(identifier.from_numeric(42));
    ASSERT_NO_THROW(identifier.from_string(value));

    ASSERT_EQ(identifier.kind, "string");
    ASSERT_EQ(identifier.length, value.size());
    ASSERT_EQ(identifier.value.size(), value.size());
    for (size_t i = 0; i < value.size(); ++i) {
        EXPECT_EQ(identifier.value[i], static_cast<std::uint8_t>(value[i]));
    }
}
