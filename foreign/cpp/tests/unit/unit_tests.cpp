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
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "iggy.hpp"

namespace {

std::string option_key(const iggy::ffi::HeaderEntry &entry) {
    return std::string(entry.key.value.begin(), entry.key.value.end());
}

std::vector<std::uint8_t> option_value_bytes(const iggy::ffi::HeaderEntry &entry) {
    return std::vector<std::uint8_t>(entry.value.value.begin(), entry.value.value.end());
}

constexpr std::uint8_t kind_code(const iggy::ffi::HeaderKind kind) {
    return static_cast<std::uint8_t>(kind);
}

}  // namespace

TEST(CompressionAlgorithmTest, ReturnsExpectedValues) {
    EXPECT_EQ(iggy::CompressionAlgorithm::none().compression_algorithm_value(), "none");
    EXPECT_EQ(iggy::CompressionAlgorithm::gzip().compression_algorithm_value(), "gzip");
}

TEST(SnapshotCompressionTest, ReturnsExpectedValues) {
    EXPECT_EQ(iggy::SnapshotCompression::stored().snapshot_compression_value(), "stored");
    EXPECT_EQ(iggy::SnapshotCompression::deflated().snapshot_compression_value(), "deflated");
    EXPECT_EQ(iggy::SnapshotCompression::bzip2().snapshot_compression_value(), "bzip2");
    EXPECT_EQ(iggy::SnapshotCompression::zstd().snapshot_compression_value(), "zstd");
    EXPECT_EQ(iggy::SnapshotCompression::lzma().snapshot_compression_value(), "lzma");
    EXPECT_EQ(iggy::SnapshotCompression::xz().snapshot_compression_value(), "xz");
}

TEST(SystemSnapshotTypeTest, ReturnsExpectedValues) {
    EXPECT_EQ(iggy::SystemSnapshotType::filesystem_overview().snapshot_type_value(), "filesystem_overview");
    EXPECT_EQ(iggy::SystemSnapshotType::process_list().snapshot_type_value(), "process_list");
    EXPECT_EQ(iggy::SystemSnapshotType::resource_usage().snapshot_type_value(), "resource_usage");
    EXPECT_EQ(iggy::SystemSnapshotType::test().snapshot_type_value(), "test");
    EXPECT_EQ(iggy::SystemSnapshotType::server_logs().snapshot_type_value(), "server_logs");
    EXPECT_EQ(iggy::SystemSnapshotType::server_config().snapshot_type_value(), "server_config");
    EXPECT_EQ(iggy::SystemSnapshotType::all().snapshot_type_value(), "all");
}

TEST(IdKindTest, ReturnsExpectedValues) {
    EXPECT_EQ(iggy::IdKind::numeric().id_kind_value(), "numeric");
    EXPECT_EQ(iggy::IdKind::string().id_kind_value(), "string");
}

TEST(MaxTopicSizeTest, ReturnsExpectedValues) {
    EXPECT_EQ(iggy::MaxTopicSize::server_default().max_topic_size(), "server_default");
    EXPECT_EQ(iggy::MaxTopicSize::unlimited().max_topic_size(), "unlimited");
    EXPECT_EQ(iggy::MaxTopicSize::from_bytes(0).max_topic_size(), "server_default");
    EXPECT_EQ(iggy::MaxTopicSize::from_bytes(std::numeric_limits<std::uint64_t>::max()).max_topic_size(), "unlimited");
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

TEST(ExpiryTest, ReturnsExpectedKindAndValue) {
    const auto server_default = iggy::Expiry::server_default();
    EXPECT_EQ(server_default.expiry_kind(), "server_default");
    EXPECT_EQ(server_default.expiry_value(), static_cast<std::uint64_t>(0));

    const auto never_expire = iggy::Expiry::never_expire();
    EXPECT_EQ(never_expire.expiry_kind(), "never_expire");
    EXPECT_EQ(never_expire.expiry_value(), std::numeric_limits<std::uint64_t>::max());

    const auto duration = iggy::Expiry::duration(15);
    EXPECT_EQ(duration.expiry_kind(), "duration");
    EXPECT_EQ(duration.expiry_value(), static_cast<std::uint64_t>(15));
}

TEST(TopicOptionTest, SegmentSizeEncodesLittleEndianUint64) {
    const auto option = iggy::TopicOption::segment_size(0x0102030405060708ULL);

    EXPECT_EQ(option.key.kind, kind_code(iggy::ffi::HeaderKind::String));
    EXPECT_EQ(option_key(option), "segment_size");
    EXPECT_EQ(option.value.kind, kind_code(iggy::ffi::HeaderKind::Uint64));
    EXPECT_EQ(option_value_bytes(option), (std::vector<std::uint8_t>{0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01}));
}

TEST(TopicOptionTest, EnforceFsyncEncodesSingleBoolByte) {
    const auto enabled = iggy::TopicOption::enforce_fsync(true);

    EXPECT_EQ(enabled.key.kind, kind_code(iggy::ffi::HeaderKind::String));
    EXPECT_EQ(option_key(enabled), "enforce_fsync");
    EXPECT_EQ(enabled.value.kind, kind_code(iggy::ffi::HeaderKind::Bool));
    EXPECT_EQ(option_value_bytes(enabled), (std::vector<std::uint8_t>{1}));

    const auto disabled = iggy::TopicOption::enforce_fsync(false);

    EXPECT_EQ(option_key(disabled), "enforce_fsync");
    EXPECT_EQ(disabled.value.kind, kind_code(iggy::ffi::HeaderKind::Bool));
    EXPECT_EQ(option_value_bytes(disabled), (std::vector<std::uint8_t>{0}));
}

TEST(TopicOptionTest, MessagesRequiredToSaveEncodesLittleEndianUint32) {
    const auto option = iggy::TopicOption::messages_required_to_save(0x01020304U);

    EXPECT_EQ(option.key.kind, kind_code(iggy::ffi::HeaderKind::String));
    EXPECT_EQ(option_key(option), "messages_required_to_save");
    EXPECT_EQ(option.value.kind, kind_code(iggy::ffi::HeaderKind::Uint32));
    EXPECT_EQ(option_value_bytes(option), (std::vector<std::uint8_t>{0x04, 0x03, 0x02, 0x01}));
}

TEST(TopicOptionTest, SizeOfMessagesRequiredToSaveEncodesLittleEndianUint64) {
    const auto option = iggy::TopicOption::size_of_messages_required_to_save(1024ULL * 1024ULL);

    EXPECT_EQ(option.key.kind, kind_code(iggy::ffi::HeaderKind::String));
    EXPECT_EQ(option_key(option), "size_of_messages_required_to_save");
    EXPECT_EQ(option.value.kind, kind_code(iggy::ffi::HeaderKind::Uint64));
    EXPECT_EQ(option_value_bytes(option), (std::vector<std::uint8_t>{0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00}));
}

TEST(TopicOptionTest, PreallocateSegmentsEncodesSingleBoolByte) {
    const auto enabled = iggy::TopicOption::preallocate_segments(true);

    EXPECT_EQ(enabled.key.kind, kind_code(iggy::ffi::HeaderKind::String));
    EXPECT_EQ(option_key(enabled), "preallocate_segments");
    EXPECT_EQ(enabled.value.kind, kind_code(iggy::ffi::HeaderKind::Bool));
    EXPECT_EQ(option_value_bytes(enabled), (std::vector<std::uint8_t>{1}));

    const auto disabled = iggy::TopicOption::preallocate_segments(false);

    EXPECT_EQ(option_value_bytes(disabled), (std::vector<std::uint8_t>{0}));
}

TEST(TopicOptionTest, MaximumValuesFillEveryValueByte) {
    const auto segment_size = iggy::TopicOption::segment_size(std::numeric_limits<std::uint64_t>::max());
    EXPECT_EQ(option_value_bytes(segment_size),
              (std::vector<std::uint8_t>{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}));

    const auto messages_required_to_save =
        iggy::TopicOption::messages_required_to_save(std::numeric_limits<std::uint32_t>::max());
    EXPECT_EQ(option_value_bytes(messages_required_to_save), (std::vector<std::uint8_t>{0xFF, 0xFF, 0xFF, 0xFF}));
}

TEST(IggyExceptionTest, StoresMessage) {
    const iggy::IggyException from_cstr("boom");
    EXPECT_EQ(std::string(from_cstr.what()), "boom");

    const std::string message = "boom2";
    const iggy::IggyException from_string(message);
    EXPECT_EQ(std::string(from_string.what()), message);
}
