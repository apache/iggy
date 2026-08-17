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
#include <utility>
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

TEST(IggyBlockingClientTest, MovedFromOperationsThrow) {
    auto client   = iggy::IggyBlockingClient::Builder().Build();
    auto moved_to = std::move(client);
    (void)moved_to;

    EXPECT_THROW(client.Connect(), iggy::IggyException);
    EXPECT_THROW(client.Disconnect(), iggy::IggyException);
    EXPECT_THROW(client.Shutdown(), iggy::IggyException);
    EXPECT_THROW(client.Login("iggy", "iggy"), iggy::IggyException);
    EXPECT_THROW(client.Logout(), iggy::IggyException);
}

TEST(IggyBlockingClientBuilderTest, RejectsTlsDomainWhenTlsIsDisabled) {
    EXPECT_THROW((void)iggy::IggyBlockingClient::Builder().WithTlsDomain("localhost").Build(), iggy::IggyException);
}

TEST(IggyBlockingClientBuilderTest, RejectsTlsCaFileWhenTlsIsDisabled) {
    EXPECT_THROW((void)iggy::IggyBlockingClient::Builder().WithTlsCaFile("ca.pem").Build(), iggy::IggyException);
}

TEST(IggyBlockingClientBuilderTest, RejectsTlsValidationWhenTlsIsDisabled) {
    EXPECT_THROW((void)iggy::IggyBlockingClient::Builder().WithTlsCertificateValidation().Build(), iggy::IggyException);
}
