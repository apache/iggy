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
#include <map>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "iggy.hpp"

namespace {

std::string option_key(const iggy::HeaderEntry &entry) {
    return std::string(entry.Key().Value().begin(), entry.Key().Value().end());
}

const std::vector<std::uint8_t> &option_value_bytes(const iggy::HeaderEntry &entry) {
    return entry.Value().Value();
}

}  // namespace

TEST(CompressionAlgorithmTest, ReturnsExpectedValues) {
    EXPECT_EQ(iggy::CompressionAlgorithm::None().Value(), "none");
    EXPECT_EQ(iggy::CompressionAlgorithm::Gzip().Value(), "gzip");
}

TEST(SnapshotCompressionTest, ReturnsExpectedValues) {
    EXPECT_EQ(iggy::SnapshotCompression::Stored().Value(), "stored");
    EXPECT_EQ(iggy::SnapshotCompression::Deflated().Value(), "deflated");
    EXPECT_EQ(iggy::SnapshotCompression::Bzip2().Value(), "bzip2");
    EXPECT_EQ(iggy::SnapshotCompression::Zstd().Value(), "zstd");
    EXPECT_EQ(iggy::SnapshotCompression::Lzma().Value(), "lzma");
    EXPECT_EQ(iggy::SnapshotCompression::Xz().Value(), "xz");
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

TEST(MaxTopicSizeTest, ReturnsExpectedValues) {
    EXPECT_EQ(iggy::MaxTopicSize::ServerDefault().Value(), "server_default");
    EXPECT_EQ(iggy::MaxTopicSize::Unlimited().Value(), "unlimited");
    EXPECT_EQ(iggy::MaxTopicSize::FromBytes(0).Value(), "server_default");
    EXPECT_EQ(iggy::MaxTopicSize::FromBytes(std::numeric_limits<std::uint64_t>::max()).Value(),
              "unlimited");
    EXPECT_EQ(iggy::MaxTopicSize::FromBytes(1024).Value(), "1024");
}

TEST(PollingStrategyTest, ReturnsExpectedKindAndValue) {
    const auto offset = iggy::PollingStrategy::Offset(7);
    EXPECT_EQ(offset.Kind(), "offset");
    EXPECT_EQ(offset.Value(), 7u);

    const auto timestamp = iggy::PollingStrategy::Timestamp(42);
    EXPECT_EQ(timestamp.Kind(), "timestamp");
    EXPECT_EQ(timestamp.Value(), 42u);

    const auto first = iggy::PollingStrategy::First();
    EXPECT_EQ(first.Kind(), "first");
    EXPECT_EQ(first.Value(), 0u);

    const auto last = iggy::PollingStrategy::Last();
    EXPECT_EQ(last.Kind(), "last");
    EXPECT_EQ(last.Value(), 0u);

    const auto next = iggy::PollingStrategy::Next();
    EXPECT_EQ(next.Kind(), "next");
    EXPECT_EQ(next.Value(), 0u);
}

TEST(ExpiryTest, ReturnsExpectedKindAndValue) {
    const auto server_default = iggy::Expiry::ServerDefault();
    EXPECT_EQ(server_default.Kind(), "server_default");
    EXPECT_EQ(server_default.Value(), static_cast<std::uint64_t>(0));

    const auto never_expire = iggy::Expiry::NeverExpire();
    EXPECT_EQ(never_expire.Kind(), "never_expire");
    EXPECT_EQ(never_expire.Value(), std::numeric_limits<std::uint64_t>::max());

    const auto duration = iggy::Expiry::Duration(15);
    EXPECT_EQ(duration.Kind(), "duration");
    EXPECT_EQ(duration.Value(), static_cast<std::uint64_t>(15));
}

TEST(TopicCreateOptionsTest, DefaultHasNoValues) {
    const iggy::TopicCreateOptions options;
    EXPECT_FALSE(options.PartitionsCount().has_value());
    EXPECT_FALSE(options.CompressionAlgorithm().has_value());
    EXPECT_FALSE(options.MessageExpiry().has_value());
    EXPECT_FALSE(options.MaxTopicSize().has_value());
    EXPECT_FALSE(options.SegmentSize().has_value());
    EXPECT_FALSE(options.EnforceFsync().has_value());
    EXPECT_FALSE(options.MessagesRequiredToSave().has_value());
    EXPECT_FALSE(options.SizeOfMessagesRequiredToSave().has_value());
    EXPECT_FALSE(options.PreallocateSegments().has_value());
    EXPECT_TRUE(options.RawEntries().empty());
}

TEST(TopicOptionTest, DurabilityEncodesCanonicalStrings) {
    const auto persisted = iggy::TopicOption::Durability(iggy::Durability::Persisted);
    EXPECT_EQ(persisted.Key().Kind(), iggy::HeaderKind::String);
    EXPECT_EQ(option_key(persisted), "durability");
    EXPECT_EQ(persisted.Value().Kind(), iggy::HeaderKind::String);
    EXPECT_EQ(option_value_bytes(persisted), (std::vector<std::uint8_t>{'p', 'e', 'r', 's', 'i', 's', 't', 'e', 'd'}));

    const auto offset = iggy::TopicOption::ConsumerOffsetDurability();
    EXPECT_EQ(offset.Key().Kind(), iggy::HeaderKind::String);
    EXPECT_EQ(option_key(offset), "consumer_offset_durability");
    EXPECT_EQ(offset.Value().Kind(), iggy::HeaderKind::String);
    EXPECT_EQ(option_value_bytes(offset),
              (std::vector<std::uint8_t>{'r', 'e', 'p', 'l', 'i', 'c', 'a', 't', 'e', 'd'}));

    EXPECT_THROW(iggy::TopicOption::Durability(static_cast<iggy::Durability>(99)), std::invalid_argument);
}

TEST(TopicCreateOptionsTest, PartitionsCountStoresValue) {
    iggy::TopicCreateOptions options;
    options.SetPartitionsCount(3);
    ASSERT_TRUE(options.PartitionsCount().has_value());
    EXPECT_EQ(*options.PartitionsCount(), 3u);
    options.SetPartitionsCount(1000);
    EXPECT_EQ(*options.PartitionsCount(), 1000u);
}

TEST(TopicCreateOptionsTest, SegmentSizeStoresValue) {
    iggy::TopicCreateOptions options;
    options.SetSegmentSize(0x0102030405060708ULL);
    ASSERT_TRUE(options.SegmentSize().has_value());
    EXPECT_EQ(*options.SegmentSize(), 0x0102030405060708ULL);
}

TEST(TopicCreateOptionsTest, EnforceFsyncStoresBool) {
    iggy::TopicCreateOptions enabled;
    enabled.SetEnforceFsync(true);
    ASSERT_TRUE(enabled.EnforceFsync().has_value());
    EXPECT_EQ(*enabled.EnforceFsync(), true);

    iggy::TopicCreateOptions disabled;
    disabled.SetEnforceFsync(false);
    ASSERT_TRUE(disabled.EnforceFsync().has_value());
    EXPECT_EQ(*disabled.EnforceFsync(), false);
}

TEST(TopicCreateOptionsTest, MessagesRequiredToSaveStoresValue) {
    iggy::TopicCreateOptions options;
    options.SetMessagesRequiredToSave(0x01020304U);
    ASSERT_TRUE(options.MessagesRequiredToSave().has_value());
    EXPECT_EQ(*options.MessagesRequiredToSave(), 0x01020304U);
}

TEST(TopicCreateOptionsTest, SizeOfMessagesRequiredToSaveStoresValue) {
    iggy::TopicCreateOptions options;
    options.SetSizeOfMessagesRequiredToSave(1024ULL * 1024ULL);
    ASSERT_TRUE(options.SizeOfMessagesRequiredToSave().has_value());
    EXPECT_EQ(*options.SizeOfMessagesRequiredToSave(), 1024ULL * 1024ULL);
}

TEST(TopicCreateOptionsTest, PreallocateSegmentsStoresBool) {
    iggy::TopicCreateOptions enabled;
    enabled.SetPreallocateSegments(true);
    ASSERT_TRUE(enabled.PreallocateSegments().has_value());
    EXPECT_EQ(*enabled.PreallocateSegments(), true);

    iggy::TopicCreateOptions disabled;
    disabled.SetPreallocateSegments(false);
    ASSERT_TRUE(disabled.PreallocateSegments().has_value());
    EXPECT_EQ(*disabled.PreallocateSegments(), false);
}

TEST(TopicCreateOptionsTest, MaximumValuesPreserved) {
    iggy::TopicCreateOptions options;
    options.SetSegmentSize(std::numeric_limits<std::uint64_t>::max());
    ASSERT_TRUE(options.SegmentSize().has_value());
    EXPECT_EQ(*options.SegmentSize(), std::numeric_limits<std::uint64_t>::max());

    options.SetMessagesRequiredToSave(std::numeric_limits<std::uint32_t>::max());
    ASSERT_TRUE(options.MessagesRequiredToSave().has_value());
    EXPECT_EQ(*options.MessagesRequiredToSave(), std::numeric_limits<std::uint32_t>::max());

    options.SetSizeOfMessagesRequiredToSave(std::numeric_limits<std::uint64_t>::max());
    ASSERT_TRUE(options.SizeOfMessagesRequiredToSave().has_value());
    EXPECT_EQ(*options.SizeOfMessagesRequiredToSave(), std::numeric_limits<std::uint64_t>::max());
}

TEST(TopicCreateOptionsTest, ChainingAndOverwrite) {
    iggy::TopicCreateOptions options;
    options.SetSegmentSize(1024).SetEnforceFsync(true).SetMessagesRequiredToSave(512);
    EXPECT_EQ(*options.SegmentSize(), 1024ULL);
    EXPECT_EQ(*options.EnforceFsync(), true);
    EXPECT_EQ(*options.MessagesRequiredToSave(), 512u);
    options.SetSegmentSize(2048);
    EXPECT_EQ(*options.SegmentSize(), 2048ULL);
}

TEST(TopicCreateOptionsTest, CompressionAlgorithmAndExpiryAndMaxTopicSize) {
    iggy::TopicCreateOptions options;
    options.SetCompressionAlgorithm(iggy::CompressionAlgorithm::Gzip())
        .SetMessageExpiry(iggy::Expiry::Duration(15))
        .SetMaxTopicSize(iggy::MaxTopicSize::FromBytes(1024));
    ASSERT_TRUE(options.CompressionAlgorithm().has_value());
    EXPECT_EQ(options.CompressionAlgorithm()->Value(), "gzip");
    ASSERT_TRUE(options.MessageExpiry().has_value());
    EXPECT_EQ(options.MessageExpiry()->Kind(), "duration");
    EXPECT_EQ(options.MessageExpiry()->Value(), 15u);
    ASSERT_TRUE(options.MaxTopicSize().has_value());
    EXPECT_EQ(options.MaxTopicSize()->Value(), "1024");
}

TEST(TopicCreateOptionsTest, ServerDefaultSentinelsClearValues) {
    iggy::TopicCreateOptions options;
    options.SetMessageExpiry(iggy::Expiry::Duration(15)).SetMaxTopicSize(iggy::MaxTopicSize::FromBytes(1024));
    ASSERT_TRUE(options.MessageExpiry().has_value());
    ASSERT_TRUE(options.MaxTopicSize().has_value());

    options.SetMessageExpiry(iggy::Expiry::ServerDefault()).SetMaxTopicSize(iggy::MaxTopicSize::ServerDefault());
    EXPECT_FALSE(options.MessageExpiry().has_value());
    EXPECT_FALSE(options.MaxTopicSize().has_value());
}

TEST(TopicCreateOptionsTest, RawMapStoresForwardCompatibleKeys) {
    iggy::TopicCreateOptions options;
    options.SetRawEntries({{"custom_key", "custom_value"}});
    EXPECT_EQ(options.RawEntries().count("custom_key"), 1u);
    EXPECT_EQ(options.RawEntries().at("custom_key"), "custom_value");
    options.SetRawEntries(std::map<std::string, std::string>{{"a", "1"}, {"b", "2"}});
    EXPECT_EQ(options.RawEntries().size(), 3u);
    EXPECT_EQ(options.RawEntries().at("a"), "1");
}

TEST(TopicCreateOptionsTest, RawMapReplacesDuplicateKeys) {
    iggy::TopicCreateOptions options;
    const std::map<std::string, std::string> first_entries{{"message_expiry", "7 days"}};
    const std::map<std::string, std::string> second_entries{{"message_expiry", "1 day"}};

    options.SetRawEntries(first_entries).SetRawEntries(second_entries);

    EXPECT_EQ(options.RawEntries().at("message_expiry"), "1 day");
}

TEST(TopicCreateOptionsTest, RawMapReplacesDuplicateKeysWhenMoved) {
    iggy::TopicCreateOptions options;
    std::map<std::string, std::string> first_entries{{"message_expiry", "7 days"}};
    std::map<std::string, std::string> second_entries{{"message_expiry", "1 day"}};

    options.SetRawEntries(std::move(first_entries)).SetRawEntries(std::move(second_entries));

    EXPECT_EQ(options.RawEntries().at("message_expiry"), "1 day");
}

TEST(TopicUpdateOptionsTest, DefaultHasNoValues) {
    const iggy::TopicUpdateOptions options;
    EXPECT_FALSE(options.CompressionAlgorithm().has_value());
    EXPECT_FALSE(options.MessageExpiry().has_value());
    EXPECT_FALSE(options.MaxTopicSize().has_value());
    EXPECT_TRUE(options.RawEntries().empty());
}

TEST(TopicUpdateOptionsTest, StoresUpdatableFields) {
    iggy::TopicUpdateOptions options;
    options.SetCompressionAlgorithm(iggy::CompressionAlgorithm::Gzip())
        .SetMessageExpiry(iggy::Expiry::NeverExpire())
        .SetMaxTopicSize(iggy::MaxTopicSize::Unlimited());
    ASSERT_TRUE(options.CompressionAlgorithm().has_value());
    EXPECT_EQ(options.CompressionAlgorithm()->Value(), "gzip");
    ASSERT_TRUE(options.MessageExpiry().has_value());
    EXPECT_EQ(options.MessageExpiry()->Kind(), "never_expire");
    ASSERT_TRUE(options.MaxTopicSize().has_value());
    EXPECT_EQ(options.MaxTopicSize()->Value(), "unlimited");
}

TEST(TopicUpdateOptionsTest, ServerDefaultSentinelsClearValues) {
    iggy::TopicUpdateOptions options;
    options.SetMessageExpiry(iggy::Expiry::Duration(15)).SetMaxTopicSize(iggy::MaxTopicSize::FromBytes(1024));
    ASSERT_TRUE(options.MessageExpiry().has_value());
    ASSERT_TRUE(options.MaxTopicSize().has_value());

    options.SetMessageExpiry(iggy::Expiry::ServerDefault()).SetMaxTopicSize(iggy::MaxTopicSize::ServerDefault());
    EXPECT_FALSE(options.MessageExpiry().has_value());
    EXPECT_FALSE(options.MaxTopicSize().has_value());
}

TEST(TopicUpdateOptionsTest, RawMapStoresKeys) {
    iggy::TopicUpdateOptions options;
    options.SetRawEntries({{"message_expiry", "7 days"}});
    EXPECT_EQ(options.RawEntries().count("message_expiry"), 1u);
    options.SetRawEntries(std::map<std::string, std::string>{{"compression_algorithm", "gzip"}});
    EXPECT_EQ(options.RawEntries().size(), 2u);
}

TEST(TopicUpdateOptionsTest, RawMapReplacesDuplicateKeys) {
    iggy::TopicUpdateOptions options;
    const std::map<std::string, std::string> first_entries{{"message_expiry", "7 days"}};
    const std::map<std::string, std::string> second_entries{{"message_expiry", "1 day"}};
    std::map<std::string, std::string> third_entries{{"message_expiry", "7 days"}};
    std::map<std::string, std::string> fourth_entries{{"message_expiry", "1 day"}};

    options.SetRawEntries(first_entries).SetRawEntries(second_entries);
    EXPECT_EQ(options.RawEntries().at("message_expiry"), "1 day");

    options.SetRawEntries(std::move(third_entries)).SetRawEntries(std::move(fourth_entries));

    EXPECT_EQ(options.RawEntries().at("message_expiry"), "1 day");
}

TEST(StreamUpdateOptionsTest, RawMapStoresKeys) {
    iggy::StreamUpdateOptions options;
    EXPECT_TRUE(options.RawEntries().empty());
    options.SetRawEntries({{"future_key", "future_value"}});
    EXPECT_EQ(options.RawEntries().count("future_key"), 1u);
    EXPECT_EQ(options.RawEntries().at("future_key"), "future_value");
}

TEST(StreamUpdateOptionsTest, RawMapReplacesDuplicateKeys) {
    iggy::StreamUpdateOptions options;
    std::map<std::string, std::string> first_entries{{"future_key", "first_value"}};
    std::map<std::string, std::string> second_entries{{"future_key", "second_value"}};
    const std::map<std::string, std::string> third_entries{{"future_key", "first_value"}};
    const std::map<std::string, std::string> fourth_entries{{"future_key", "second_value"}};

    options.SetRawEntries(std::move(first_entries)).SetRawEntries(std::move(second_entries));
    EXPECT_EQ(options.RawEntries().at("future_key"), "second_value");

    options.SetRawEntries(third_entries).SetRawEntries(fourth_entries);

    EXPECT_EQ(options.RawEntries().at("future_key"), "second_value");
}

TEST(IggyExceptionTest, StoresMessage) {
    const iggy::IggyException from_cstr("boom");
    EXPECT_EQ(std::string(from_cstr.what()), "boom");

    const std::string message = "boom2";
    const iggy::IggyException from_string(message);
    EXPECT_EQ(std::string(from_string.what()), message);
}

TEST(AutoLoginKindTest, HasStableDiscriminantsAndZeroInitializedDefault) {
    EXPECT_EQ(static_cast<std::uint8_t>(iggy::ffi::AutoLoginKind::Disabled), 0u);
    EXPECT_EQ(static_cast<std::uint8_t>(iggy::ffi::AutoLoginKind::UsernamePassword), 1u);
    EXPECT_EQ(static_cast<std::uint8_t>(iggy::ffi::AutoLoginKind::PersonalAccessToken), 2u);

    const iggy::ffi::IggyClientConfig config{};
    EXPECT_EQ(config.auto_login_kind, iggy::ffi::AutoLoginKind::Disabled);
}

TEST(IggyBlockingClientBuilderTest, BuildsWithEachAutoLoginKind) {
    EXPECT_NO_THROW((void)iggy::IggyBlockingClient::Builder().Build());
    EXPECT_NO_THROW((void)iggy::IggyBlockingClient::Builder().WithAutoLogin("iggy", "iggy").Build());
    EXPECT_NO_THROW((void)iggy::IggyBlockingClient::Builder().WithPersonalAccessToken("token").Build());
}

TEST(IggyBlockingClientBuilderTest, RejectsEmptyServerAddress) {
    EXPECT_THROW((void)iggy::IggyBlockingClient::Builder().WithServerAddress(""), iggy::IggyException);
}

TEST(IggyBlockingClientBuilderTest, RejectsEmptyAutoLoginCredentials) {
    EXPECT_THROW((void)iggy::IggyBlockingClient::Builder().WithAutoLogin("", "password"), iggy::IggyException);
    EXPECT_THROW((void)iggy::IggyBlockingClient::Builder().WithAutoLogin("username", ""), iggy::IggyException);
}

TEST(IggyBlockingClientBuilderTest, RejectsEmptyPersonalAccessToken) {
    EXPECT_THROW((void)iggy::IggyBlockingClient::Builder().WithPersonalAccessToken(""), iggy::IggyException);
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

TEST(IggyBlockingClientBuilderTest, RejectsEmptyTlsSettings) {
    EXPECT_THROW((void)iggy::IggyBlockingClient::Builder().WithTlsDomain(""), iggy::IggyException);
    EXPECT_THROW((void)iggy::IggyBlockingClient::Builder().WithTlsCaFile(""), iggy::IggyException);
}
