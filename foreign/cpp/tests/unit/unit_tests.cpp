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

#include <string>

#include <gtest/gtest.h>

#include "iggy.hpp"

TEST(CompressionAlgorithmTest, ReturnsExpectedValues) {
    EXPECT_EQ(iggy::CompressionAlgorithm::None().CompressionAlgorithmValue(), "none");
    EXPECT_EQ(iggy::CompressionAlgorithm::Gzip().CompressionAlgorithmValue(), "gzip");
}

TEST(SnapshotCompressionTest, ReturnsExpectedValues) {
    EXPECT_EQ(iggy::SnapshotCompression::Stored().SnapshotCompressionValue(), "stored");
    EXPECT_EQ(iggy::SnapshotCompression::Deflated().SnapshotCompressionValue(), "deflated");
    EXPECT_EQ(iggy::SnapshotCompression::Bzip2().SnapshotCompressionValue(), "bzip2");
    EXPECT_EQ(iggy::SnapshotCompression::Zstd().SnapshotCompressionValue(), "zstd");
    EXPECT_EQ(iggy::SnapshotCompression::Lzma().SnapshotCompressionValue(), "lzma");
    EXPECT_EQ(iggy::SnapshotCompression::Xz().SnapshotCompressionValue(), "xz");
}

TEST(SystemSnapshotTypeTest, ReturnsExpectedValues) {
    EXPECT_EQ(iggy::SystemSnapshotType::FilesystemOverview().SnapshotTypeValue(), "filesystem_overview");
    EXPECT_EQ(iggy::SystemSnapshotType::ProcessList().SnapshotTypeValue(), "process_list");
    EXPECT_EQ(iggy::SystemSnapshotType::ResourceUsage().SnapshotTypeValue(), "resource_usage");
    EXPECT_EQ(iggy::SystemSnapshotType::Test().SnapshotTypeValue(), "test");
    EXPECT_EQ(iggy::SystemSnapshotType::ServerLogs().SnapshotTypeValue(), "server_logs");
    EXPECT_EQ(iggy::SystemSnapshotType::ServerConfig().SnapshotTypeValue(), "server_config");
    EXPECT_EQ(iggy::SystemSnapshotType::All().SnapshotTypeValue(), "all");
}

TEST(IdKindTest, ReturnsExpectedValues) {
    EXPECT_EQ(iggy::IdKind::Numeric().IdKindValue(), "numeric");
    EXPECT_EQ(iggy::IdKind::String().IdKindValue(), "string");
}

TEST(MaxTopicSizeTest, ReturnsExpectedValues) {
    EXPECT_EQ(iggy::MaxTopicSize::ServerDefault().MaxTopicSizeValue(), "server_default");
    EXPECT_EQ(iggy::MaxTopicSize::Unlimited().MaxTopicSizeValue(), "unlimited");
    EXPECT_EQ(iggy::MaxTopicSize::FromBytes(0).MaxTopicSizeValue(), "server_default");
    EXPECT_EQ(iggy::MaxTopicSize::FromBytes(std::numeric_limits<std::uint64_t>::max()).MaxTopicSizeValue(),
              "unlimited");
    EXPECT_EQ(iggy::MaxTopicSize::FromBytes(1024).MaxTopicSizeValue(), "1024");
}

TEST(PollingStrategyTest, ReturnsExpectedKindAndValue) {
    const auto offset = iggy::PollingStrategy::Offset(7);
    EXPECT_EQ(offset.PollingStrategyKind(), "offset");
    EXPECT_EQ(offset.PollingStrategyValue(), 7u);

    const auto timestamp = iggy::PollingStrategy::Timestamp(42);
    EXPECT_EQ(timestamp.PollingStrategyKind(), "timestamp");
    EXPECT_EQ(timestamp.PollingStrategyValue(), 42u);

    const auto first = iggy::PollingStrategy::First();
    EXPECT_EQ(first.PollingStrategyKind(), "first");
    EXPECT_EQ(first.PollingStrategyValue(), 0u);

    const auto last = iggy::PollingStrategy::Last();
    EXPECT_EQ(last.PollingStrategyKind(), "last");
    EXPECT_EQ(last.PollingStrategyValue(), 0u);

    const auto next = iggy::PollingStrategy::Next();
    EXPECT_EQ(next.PollingStrategyKind(), "next");
    EXPECT_EQ(next.PollingStrategyValue(), 0u);
}

TEST(ExpiryTest, ReturnsExpectedKindAndValue) {
    const auto server_default = iggy::Expiry::ServerDefault();
    EXPECT_EQ(server_default.ExpiryKind(), "server_default");
    EXPECT_EQ(server_default.ExpiryValue(), static_cast<std::uint64_t>(0));

    const auto never_expire = iggy::Expiry::NeverExpire();
    EXPECT_EQ(never_expire.ExpiryKind(), "never_expire");
    EXPECT_EQ(never_expire.ExpiryValue(), std::numeric_limits<std::uint64_t>::max());

    const auto duration = iggy::Expiry::Duration(15);
    EXPECT_EQ(duration.ExpiryKind(), "duration");
    EXPECT_EQ(duration.ExpiryValue(), static_cast<std::uint64_t>(15));
}

TEST(IggyExceptionTest, StoresMessage) {
    const iggy::IggyException from_cstr("boom");
    EXPECT_EQ(std::string(from_cstr.what()), "boom");

    const std::string message = "boom2";
    const iggy::IggyException from_string(message);
    EXPECT_EQ(std::string(from_string.what()), message);
}
