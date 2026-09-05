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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "iggy.hpp"
#include "lib.rs.h"
#include "tests/e2e/test_helpers.hpp"

class E2E_Topic : public E2ETestFixture {};

TEST_F(E2E_Topic, CreateTopicWithAllOptionCombinations) {
    RecordProperty("description",
                   "Creates topics across supported option combinations and verifies they are all returned.");
    const std::string stream_name = GetRandomName();

    auto client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);

    struct CompressionOption {
        std::string name;
        iggy::CompressionAlgorithm value;
    };
    const std::vector<CompressionOption> compression_algorithms = {
        {"none", iggy::CompressionAlgorithm::None()},
        {"gzip", iggy::CompressionAlgorithm::Gzip()},
    };
    struct ExpiryOption {
        std::string name;
        iggy::Expiry value;
    };
    const std::vector<ExpiryOption> expiry_options = {
        {"server_default", iggy::Expiry::ServerDefault()},
        {"never_expire", iggy::Expiry::NeverExpire()},
        {"duration", iggy::Expiry::Duration(1000)},
    };
    struct MaxTopicSizeOption {
        std::string name;
        iggy::MaxTopicSize value;
    };
    const std::vector<MaxTopicSizeOption> max_topic_sizes = {
        {"server_default", iggy::MaxTopicSize::ServerDefault()},
        {"unlimited", iggy::MaxTopicSize::Unlimited()},
        {"1GiB", iggy::MaxTopicSize::FromBytes(1024ULL * 1024ULL * 1024ULL)},
    };

    std::size_t expected_topics_count = 0;
    std::unordered_set<std::string> expected_topic_names;
    for (const auto &compression_algorithm : compression_algorithms) {
        for (const auto &expiry_option : expiry_options) {
            for (const auto &max_topic_size : max_topic_sizes) {
                const std::string topic_name = GetRandomName();
                SCOPED_TRACE("compression=" + compression_algorithm.name + ", expiry_kind=" + expiry_option.name +
                             ", max_topic_size=" + max_topic_size.name);

                iggy::TopicCreateOptions options;
                options.SetPartitionsCount(1)
                    .SetCompressionAlgorithm(compression_algorithm.value)
                    .SetMessageExpiry(expiry_option.value)
                    .SetMaxTopicSize(max_topic_size.value);
                ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name, options));
                ++expected_topics_count;
                expected_topic_names.insert(topic_name);
            }
        }
    }

    ASSERT_NO_THROW({
        const auto stream_details = client.GetStream(iggy::Identifier::String(stream_name));
        EXPECT_EQ(stream_details.Name(), stream_name);
        EXPECT_EQ(stream_details.TopicsCount(), expected_topics_count);
        ASSERT_EQ(stream_details.Topics().size(), expected_topics_count);
        for (const auto &topic : stream_details.Topics()) {
            const auto erased = expected_topic_names.erase(topic.Name());
            EXPECT_EQ(erased, 1u) << "Unexpected topic name returned: " << topic.Name();
        }
        EXPECT_TRUE(expected_topic_names.empty());
    });
}

TEST_F(E2E_Topic, CreateTopicWithBoundaryPartitionsCountValues) {
    RecordProperty("description", "Accepts boundary partition counts and rejects values above the supported maximum.");
    const std::string stream_name                = GetRandomName();
    const std::string default_partitions_topic   = GetRandomName();
    const std::string max_partitions_topic_name  = GetRandomName();
    const std::string overflow_topic_name        = GetRandomName();
    const std::string zero_partitions_topic_name = GetRandomName();

    auto client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);

    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), default_partitions_topic,
                                       iggy::TopicCreateOptions()));
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), max_partitions_topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1000)));
    ASSERT_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), overflow_topic_name,
                                    iggy::TopicCreateOptions().SetPartitionsCount(1001)),
                 std::exception);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), zero_partitions_topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(0)));

    const auto stream_details = client.GetStream(iggy::Identifier::String(stream_name));
    EXPECT_EQ(stream_details.TopicsCount(), 3u);

    std::unordered_map<std::string, std::uint32_t> topic_partitions;
    for (const auto &topic : stream_details.Topics()) {
        topic_partitions[topic.Name()] = topic.PartitionsCount();
    }

    EXPECT_EQ(topic_partitions.size(), 3u);
    EXPECT_EQ(topic_partitions[default_partitions_topic], 1u);
    EXPECT_EQ(topic_partitions[max_partitions_topic_name], 1000u);
    EXPECT_EQ(topic_partitions[zero_partitions_topic_name], 0u);
}

TEST_F(E2E_Topic, CreateTopicWithInvalidNamesThrows) {
    RecordProperty("description", "Rejects invalid topic names and accepts the maximum allowed name length.");
    const std::string stream_name = GetRandomName();

    auto client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);

    const std::string illegal_topic_names[] = {
        "",
        std::string(256, 'b'),
    };
    for (const auto &topic_name : illegal_topic_names) {
        SCOPED_TRACE(topic_name);
        ASSERT_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                        iggy::TopicCreateOptions().SetPartitionsCount(1)),
                     std::exception);
    }

    const std::string max_length_name(255, 'a');
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), max_length_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
}

TEST_F(E2E_Topic, CreateDuplicateTopicThrows) {
    RecordProperty("description", "Rejects creating a duplicate topic within the same stream.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    auto client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    ASSERT_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                    iggy::TopicCreateOptions().SetPartitionsCount(1)),
                 std::exception);
}

TEST_F(E2E_Topic, CreateSameTopicNameInDifferentStreamsSucceeds) {
    RecordProperty("description", "Allows the same topic name to be created in different streams.");
    const std::string first_stream_name  = GetRandomName();
    const std::string second_stream_name = GetRandomName();
    const std::string topic_name         = GetRandomName();

    auto client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(first_stream_name));
    TrackStream(first_stream_name);
    ASSERT_NO_THROW(client.CreateStream(second_stream_name));
    TrackStream(second_stream_name);

    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(first_stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(second_stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
}

TEST_F(E2E_Topic, CreateTopicWithInvalidOptionsThrows) {
    RecordProperty("description", "Rejects topic creation requests that use invalid option values.");
    const std::string stream_name                    = GetRandomName();
    const std::string invalid_compression_topic_name = GetRandomName();
    const std::string invalid_expiry_topic_name      = GetRandomName();
    const std::string invalid_max_size_topic_name    = GetRandomName();

    auto client = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);

    ASSERT_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), invalid_compression_topic_name,
                                    iggy::TopicCreateOptions().SetPartitionsCount(1).SetRawEntries(
                                        {{"compression_algorithm", "invalid-compression"}})),
                 std::exception);
    ASSERT_THROW(client.CreateTopic(
                     iggy::Identifier::String(stream_name), invalid_expiry_topic_name,
                     iggy::TopicCreateOptions().SetPartitionsCount(1).SetRawEntries({{"message_expiry", "invalid"}})),
                 std::exception);
    ASSERT_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), invalid_max_size_topic_name,
                                    iggy::TopicCreateOptions().SetPartitionsCount(1).SetRawEntries(
                                        {{"max_topic_size", "not-a-size"}})),
                 std::exception);
}

TEST_F(E2E_Topic, CreateTopicWithOptionsReturnsCanonicalKindAndDerivedRemainder) {
    RecordProperty("description",
                   "Returns an explicitly set option in its canonical kind, derives the keys left unset, and rejects "
                   "an option key outside the server catalog.");
    const std::string stream_name          = GetRandomName();
    const std::string topic_name           = GetRandomName();
    const std::string unknown_option_topic = GetRandomName();

    auto client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);

    iggy::TopicCreateOptions options;
    options.SetPartitionsCount(1).SetRawEntries({{"enforce_fsync", "true"}});
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name, options));

    const auto topic_details =
        client.GetTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name));

    // Admission re-encodes the block from its own parse, so a value comes back
    // in its key's catalog kind rather than in the kind that was sent.
    const auto &explicit_options = topic_details.Options().Explicit();
    ASSERT_EQ(explicit_options.size(), 1u);
    auto explicit_it = explicit_options.find("enforce_fsync");
    ASSERT_NE(explicit_it, explicit_options.end());
    EXPECT_EQ(explicit_it->second.Kind(), iggy::HeaderKind::Bool);
    EXPECT_EQ(explicit_it->second.Value(), (std::vector<std::uint8_t>{1}));

    EXPECT_FALSE(topic_details.Options().Derived().empty());
    const auto &derived_options = topic_details.Options().Derived();
    EXPECT_EQ(derived_options.count("max_topic_size"), 1u);
    EXPECT_EQ(derived_options.count("enforce_fsync"), 0u);

    iggy::TopicCreateOptions unknown_options;
    unknown_options.SetPartitionsCount(1).SetRawEntries({{"not_a_real_option", "true"}});
    ASSERT_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), unknown_option_topic, unknown_options),
                 std::exception);
}

TEST_F(E2E_Topic, CreateTopicWithTypedOptionHelpersReportsThemAsExplicitOptions) {
    RecordProperty("description",
                   "Creates a topic with every typed option helper and verifies each key comes back as an explicit "
                   "option in its catalog kind.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    auto client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);

    constexpr std::uint64_t segment_size_bytes                = 8ULL * 1024ULL * 1024ULL;
    constexpr std::uint32_t messages_required_to_save         = 512;
    constexpr std::uint64_t size_of_messages_required_to_save = 2ULL * 1024ULL * 1024ULL;

    iggy::TopicCreateOptions options;
    options.SetPartitionsCount(1)
        .SetSegmentSize(segment_size_bytes)
        .SetEnforceFsync(true)
        .SetMessagesRequiredToSave(messages_required_to_save)
        .SetSizeOfMessagesRequiredToSave(size_of_messages_required_to_save)
        .SetPreallocateSegments(false);

    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name, options));

    const auto topic_details =
        client.GetTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name));

    const auto &explicit_options = topic_details.Options().Explicit();
    EXPECT_EQ(explicit_options.count("segment_size"), 1u);
    EXPECT_EQ(explicit_options.count("enforce_fsync"), 1u);
    EXPECT_EQ(explicit_options.count("messages_required_to_save"), 1u);
    EXPECT_EQ(explicit_options.count("size_of_messages_required_to_save"), 1u);
    EXPECT_EQ(explicit_options.count("preallocate_segments"), 1u);

    auto check_kind = [&](const std::string &key, iggy::HeaderKind expected) {
        auto it = explicit_options.find(key);
        ASSERT_NE(it, explicit_options.end()) << "Missing explicit key: " << key;
        EXPECT_EQ(it->second.Kind(), expected) << "Wrong kind for key: " << key;
    };
    check_kind("segment_size", iggy::HeaderKind::Uint64);
    check_kind("enforce_fsync", iggy::HeaderKind::Bool);
    check_kind("messages_required_to_save", iggy::HeaderKind::Uint32);
    check_kind("size_of_messages_required_to_save", iggy::HeaderKind::Uint64);
    check_kind("preallocate_segments", iggy::HeaderKind::Bool);

    EXPECT_EQ(explicit_options.size(), 5u);
    EXPECT_EQ(topic_details.Options().Derived().count("segment_size"), 0u)
        << "segment_size was set explicitly, so it cannot be derived";
}

TEST_F(E2E_Topic, DescribeOptionsServesTopicCatalogAndRejectsUnknownScope) {
    RecordProperty("description",
                   "Serves the topic option catalog with each key's kind, default and description, returns an empty "
                   "catalog for the stream scope, and rejects an unknown scope name.");

    iggy::ffi::Client *client = GetLoggedInClient();

    rust::Vec<iggy::ffi::OptionSpec> topic_options;
    ASSERT_NO_THROW({ topic_options = client->describe_options("topic"); });

    const iggy::ffi::OptionSpec *segment_size = nullptr;
    bool found_enforce_fsync                  = false;
    for (const auto &option : topic_options) {
        const std::string key = static_cast<std::string>(option.key);
        if (key == "segment_size") {
            segment_size = &option;
        } else if (key == "enforce_fsync") {
            found_enforce_fsync = true;
        }
    }

    ASSERT_NE(segment_size, nullptr) << "Topic catalog is missing segment_size";
    EXPECT_TRUE(found_enforce_fsync) << "Topic catalog is missing enforce_fsync";
    EXPECT_EQ(segment_size->kind, static_cast<std::uint8_t>(iggy::ffi::HeaderKind::Uint64));
    EXPECT_FALSE(segment_size->default_value.empty());
    EXPECT_FALSE(segment_size->description.empty());

    // Streams take no option keys yet, which is an empty catalog rather than a failure.
    ASSERT_NO_THROW({
        const auto stream_options = client->describe_options("stream");
        EXPECT_TRUE(stream_options.empty());
    });

    ASSERT_THROW(client->describe_options("not_a_scope"), std::exception);
}

TEST_F(E2E_Topic, CreateTopicWithMaxTopicSizeBelowSegmentSizeThrows) {
    RecordProperty("description",
                   "Rejects topic creation when the maximum topic size is smaller than the segment size.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    auto client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    iggy::TopicCreateOptions options;
    options.SetPartitionsCount(1).SetMaxTopicSize(iggy::MaxTopicSize::FromBytes(1024));
    ASSERT_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name, options), std::exception);
}

TEST_F(E2E_Topic, CreateTopicOnNonExistentStreamThrows) {
    RecordProperty("description", "Throws when creating a topic on a stream that does not exist.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    auto client = GetLoggedInHighLevelClient();
    ASSERT_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                    iggy::TopicCreateOptions().SetPartitionsCount(1)),
                 std::exception);
}

TEST_F(E2E_Topic, CreateTopicAfterStreamDeletionThrows) {
    RecordProperty("description", "Throws when creating a topic after its stream has been deleted.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    auto client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.DeleteStream(iggy::Identifier::String(stream_name)));
    ForgetTrackedStream(stream_name);

    ASSERT_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                    iggy::TopicCreateOptions().SetPartitionsCount(1)),
                 std::exception);
}

TEST_F(E2E_Topic, CreateTopicBeforeLoginThrows) {
    RecordProperty("description", "Throws when topic creation is attempted from an unauthenticated client.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    auto client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);

    auto unauthenticated_client = GetLoggedOutHighLevelClient();

    ASSERT_THROW(unauthenticated_client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                                    iggy::TopicCreateOptions().SetPartitionsCount(1)),
                 std::exception);
    ASSERT_NO_THROW(unauthenticated_client.Connect());
    ASSERT_THROW(unauthenticated_client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                                    iggy::TopicCreateOptions().SetPartitionsCount(1)),
                 std::exception);
    ASSERT_NO_THROW(unauthenticated_client.Login("iggy", "iggy"));
    ASSERT_NO_THROW(unauthenticated_client.Disconnect());
    ASSERT_THROW(unauthenticated_client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                                    iggy::TopicCreateOptions().SetPartitionsCount(1)),
                 std::exception);
}

TEST_F(E2E_Topic, DeleteTopicAfterCreate) {
    RecordProperty("description", "Deletes an existing topic after creating it.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    auto client = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));

    ASSERT_NO_THROW(client.DeleteTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name)));

    ASSERT_NO_THROW({
        const auto topics = client.GetTopics(iggy::Identifier::String(stream_name));
        EXPECT_TRUE(topics.empty());
    });
}

TEST_F(E2E_Topic, DeleteTopicOnNonExistentStreamThrows) {
    RecordProperty("description", "Throws when deleting a topic from a stream that does not exist.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    auto client = GetLoggedInHighLevelClient();

    ASSERT_THROW(client.DeleteTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name)),
                 std::exception);
}

TEST_F(E2E_Topic, DeleteTopicOnNonExistentTopicThrows) {
    RecordProperty("description", "Throws when deleting a topic that does not exist.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    auto client = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);

    ASSERT_THROW(client.DeleteTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name)),
                 std::exception);
}

TEST_F(E2E_Topic, DeleteTopicTwiceThrows) {
    RecordProperty("description", "Throws when deleting the same topic a second time.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    auto client = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));

    ASSERT_NO_THROW(client.DeleteTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name)));
    ASSERT_THROW(client.DeleteTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name)),
                 std::exception);
}

TEST_F(E2E_Topic, DeleteTopicAfterStreamDeletionThrows) {
    RecordProperty("description", "Throws when deleting a topic after its stream has been deleted.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    auto client = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    ASSERT_NO_THROW(client.DeleteStream(iggy::Identifier::String(stream_name)));
    ForgetTrackedStream(stream_name);

    ASSERT_THROW(client.DeleteTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name)),
                 std::exception);
}

TEST_F(E2E_Topic, DeleteTopicBeforeLoginThrows) {
    RecordProperty("description", "Rejects delete_topic before connect, and after connect but before login.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    auto client = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));

    auto unauthenticated_client = GetLoggedOutHighLevelClient();

    ASSERT_THROW(
        unauthenticated_client.DeleteTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name)),
        std::exception);
    ASSERT_NO_THROW(unauthenticated_client.Connect());
    ASSERT_THROW(
        unauthenticated_client.DeleteTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name)),
        std::exception);
    ASSERT_NO_THROW(unauthenticated_client.Login("iggy", "iggy"));
    ASSERT_NO_THROW(unauthenticated_client.Disconnect());
    ASSERT_THROW(
        unauthenticated_client.DeleteTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name)),
        std::exception);
}

TEST_F(E2E_Topic, GetTopicReturnsTopicForExistingTopic) {
    RecordProperty("description", "Returns topic details for an existing topic.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    auto client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);

    ASSERT_NO_THROW(
        client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                           iggy::TopicCreateOptions()
                               .SetPartitionsCount(3)
                               .SetCompressionAlgorithm(iggy::CompressionAlgorithm::Gzip())
                               .SetMessageExpiry(iggy::Expiry::Duration(1000))
                               .SetMaxTopicSize(iggy::MaxTopicSize::FromBytes(1024ULL * 1024ULL * 1024ULL))));

    ASSERT_NO_THROW({
        const auto topic_details =
            client.GetTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name));
        EXPECT_EQ(topic_details.Name(), topic_name);
        EXPECT_EQ(topic_details.PartitionsCount(), 3u);
        EXPECT_EQ(topic_details.Partitions().size(), 3u);
        EXPECT_EQ(topic_details.CompressionAlgorithm(), "gzip");
        EXPECT_EQ(topic_details.MessageExpiry(), 1000u);
        EXPECT_EQ(topic_details.MaxTopicSize(), 1024ULL * 1024ULL * 1024ULL);
    });
}

TEST_F(E2E_Topic, GetTopicBeforeLoginThrows) {
    RecordProperty("description", "Rejects get_topic before connect, and after connect but before login.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    auto client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));

    auto unauthenticated_client = GetLoggedOutHighLevelClient();

    ASSERT_THROW(
        unauthenticated_client.GetTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name)),
        std::exception);
    ASSERT_NO_THROW(unauthenticated_client.Connect());
    ASSERT_THROW(
        unauthenticated_client.GetTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name)),
        std::exception);
    ASSERT_NO_THROW(unauthenticated_client.Login("iggy", "iggy"));
    ASSERT_NO_THROW(unauthenticated_client.Disconnect());
    ASSERT_THROW(
        unauthenticated_client.GetTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name)),
        std::exception);
}

TEST_F(E2E_Topic, GetTopicWithWrongStreamIdThrows) {
    RecordProperty("description", "Rejects get_topic when the topic belongs to a different stream.");
    const std::string first_stream_name  = GetRandomName();
    const std::string second_stream_name = GetRandomName();
    const std::string topic_name         = GetRandomName();

    auto client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(first_stream_name));
    TrackStream(first_stream_name);
    ASSERT_NO_THROW(client.CreateStream(second_stream_name));
    TrackStream(second_stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(first_stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));

    ASSERT_THROW(client.GetTopic(iggy::Identifier::String(second_stream_name), iggy::Identifier::String(topic_name)),
                 std::exception);
}

TEST_F(E2E_Topic, GetTopicWithWrongTopicThrows) {
    RecordProperty("description", "Rejects get_topic when the topic does not exist in the stream.");
    const std::string stream_name      = GetRandomName();
    const std::string topic_name       = GetRandomName();
    const std::string wrong_topic_name = GetRandomName();

    auto client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));

    ASSERT_THROW(client.GetTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(wrong_topic_name)),
                 std::exception);
}

TEST_F(E2E_Topic, GetTopicAfterStreamDeletionThrows) {
    RecordProperty("description", "Rejects get_topic after the stream has been deleted.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    auto client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    ASSERT_NO_THROW(client.DeleteStream(iggy::Identifier::String(stream_name)));
    ForgetTrackedStream(stream_name);

    ASSERT_THROW(client.GetTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name)),
                 std::exception);
}

TEST_F(E2E_Topic, GetTopicAfterTopicDeletionThrows) {
    RecordProperty("description", "Rejects get_topic after the topic has been deleted.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    auto client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    ASSERT_NO_THROW(client.DeleteTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name)));

    ASSERT_THROW(client.GetTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name)),
                 std::exception);
}

TEST_F(E2E_Topic, GetTopicReturnsEmptyPartitionsForZeroPartitionTopic) {
    RecordProperty("description", "Returns an empty partitions vector for a topic created with zero partitions.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    auto client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(0)));

    ASSERT_NO_THROW({
        const auto topic_details =
            client.GetTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name));
        EXPECT_EQ(topic_details.Name(), topic_name);
        EXPECT_EQ(topic_details.PartitionsCount(), 0u);
        EXPECT_TRUE(topic_details.Partitions().empty());
    });
}

TEST_F(E2E_Topic, GetTopicReturnsMaxBoundaryPartitionCount) {
    RecordProperty("description", "Returns the maximum boundary partition count for a topic created with it.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    auto client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1000)));

    ASSERT_NO_THROW({
        const auto topic_details =
            client.GetTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name));
        EXPECT_EQ(topic_details.Name(), topic_name);
        EXPECT_EQ(topic_details.PartitionsCount(), 1000u);
        EXPECT_EQ(topic_details.Partitions().size(), 1000u);
    });
}

TEST_F(E2E_Topic, GetTopicIsStableAcrossBackToBackCalls) {
    RecordProperty("description", "Returns stable topic details across back-to-back get_topic calls.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    auto client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(
        client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                           iggy::TopicCreateOptions()
                               .SetPartitionsCount(3)
                               .SetCompressionAlgorithm(iggy::CompressionAlgorithm::Gzip())
                               .SetMessageExpiry(iggy::Expiry::Duration(1000))
                               .SetMaxTopicSize(iggy::MaxTopicSize::FromBytes(1024ULL * 1024ULL * 1024ULL))));

    ASSERT_NO_THROW({
        const auto first_topic =
            client.GetTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name));
        const auto second_topic =
            client.GetTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name));

        EXPECT_EQ(second_topic.Name(), first_topic.Name());
        EXPECT_EQ(second_topic.MessageExpiry(), first_topic.MessageExpiry());
        EXPECT_EQ(second_topic.CompressionAlgorithm(), first_topic.CompressionAlgorithm());
        EXPECT_EQ(second_topic.MaxTopicSize(), first_topic.MaxTopicSize());
        EXPECT_EQ(second_topic.PartitionsCount(), first_topic.PartitionsCount());
        EXPECT_EQ(second_topic.Partitions().size(), first_topic.Partitions().size());
    });
}

TEST_F(E2E_Topic, GetTopicAgreesWithGetStreamTopicSummary) {
    RecordProperty("description", "Returns topic details that agree with get_stream topic summary fields.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    auto client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(
        client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                           iggy::TopicCreateOptions()
                               .SetPartitionsCount(3)
                               .SetCompressionAlgorithm(iggy::CompressionAlgorithm::Gzip())
                               .SetMessageExpiry(iggy::Expiry::Duration(1000))
                               .SetMaxTopicSize(iggy::MaxTopicSize::FromBytes(1024ULL * 1024ULL * 1024ULL))));

    ASSERT_NO_THROW({
        const auto stream_details = client.GetStream(iggy::Identifier::String(stream_name));
        ASSERT_EQ(stream_details.Topics().size(), 1u);

        const auto &topic_summary = stream_details.Topics().front();
        const auto topic_details =
            client.GetTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name));

        EXPECT_EQ(topic_details.Name(), topic_summary.Name());
        EXPECT_EQ(topic_details.MessageExpiry(), topic_summary.MessageExpiry());
        EXPECT_EQ(topic_details.CompressionAlgorithm(), topic_summary.CompressionAlgorithm());
        EXPECT_EQ(topic_details.MaxTopicSize(), topic_summary.MaxTopicSize());
        EXPECT_EQ(topic_details.PartitionsCount(), topic_summary.PartitionsCount());
        EXPECT_EQ(topic_details.Partitions().size(), topic_summary.PartitionsCount());
    });
}

TEST_F(E2E_Topic, GetTopicsReturnsCreatedTopicInputFields) {
    RecordProperty("description", "Creates topics in a stream, gets them, and verifies user-provided topic fields.");
    const std::string stream_name       = GetRandomName();
    const std::string first_topic_name  = GetRandomName();
    const std::string second_topic_name = GetRandomName();

    struct ExpectedTopic {
        std::uint32_t partitions_count;
        std::string compression_algorithm;
        std::uint64_t message_expiry;
        std::uint64_t max_topic_size;
    };

    const std::unordered_map<std::string, ExpectedTopic> expected_topics = {
        {first_topic_name, {2, "gzip", 1000, 1024ULL * 1024ULL * 1024ULL}},
        {second_topic_name,
         {1, "none", std::numeric_limits<std::uint64_t>::max(), std::numeric_limits<std::uint64_t>::max()}},
    };

    auto client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(
        client.CreateTopic(iggy::Identifier::String(stream_name), first_topic_name,
                           iggy::TopicCreateOptions()
                               .SetPartitionsCount(2)
                               .SetCompressionAlgorithm(iggy::CompressionAlgorithm::Gzip())
                               .SetMessageExpiry(iggy::Expiry::Duration(1000))
                               .SetMaxTopicSize(iggy::MaxTopicSize::FromBytes(1024ULL * 1024ULL * 1024ULL))));
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), second_topic_name,
                                       iggy::TopicCreateOptions()
                                           .SetPartitionsCount(1)
                                           .SetCompressionAlgorithm(iggy::CompressionAlgorithm::None())
                                           .SetMessageExpiry(iggy::Expiry::NeverExpire())
                                           .SetMaxTopicSize(iggy::MaxTopicSize::Unlimited())));

    ASSERT_NO_THROW({
        const auto topics = client.GetTopics(iggy::Identifier::String(stream_name));
        ASSERT_EQ(topics.size(), expected_topics.size());
        EXPECT_EQ(topics[0].Name(), first_topic_name);
        EXPECT_EQ(topics[1].Name(), second_topic_name);

        std::unordered_set<std::string> found_topic_names;
        for (const auto &topic : topics) {
            const auto expected = expected_topics.find(topic.Name());
            ASSERT_NE(expected, expected_topics.end()) << "Unexpected topic name returned: " << topic.Name();

            EXPECT_EQ(topic.Name(), expected->first);
            EXPECT_EQ(topic.PartitionsCount(), expected->second.partitions_count);
            EXPECT_EQ(topic.CompressionAlgorithm(), expected->second.compression_algorithm);
            EXPECT_EQ(topic.MessageExpiry(), expected->second.message_expiry);
            EXPECT_EQ(topic.MaxTopicSize(), expected->second.max_topic_size);
            found_topic_names.insert(topic.Name());
        }
        EXPECT_EQ(found_topic_names.size(), expected_topics.size());
    });
}

TEST_F(E2E_Topic, GetTopicsBeforeLoginThrows) {
    RecordProperty("description", "Rejects get_topics before connect, and after connect but before login.");
    const std::string stream_name = GetRandomName();

    auto setup_client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(setup_client.CreateStream(stream_name));
    TrackStream(stream_name);

    auto unauthenticated_client = GetLoggedOutHighLevelClient();

    ASSERT_THROW(unauthenticated_client.GetTopics(iggy::Identifier::String(stream_name)), std::exception);
    ASSERT_NO_THROW(unauthenticated_client.Connect());
    ASSERT_THROW(unauthenticated_client.GetTopics(iggy::Identifier::String(stream_name)), std::exception);
    ASSERT_NO_THROW(unauthenticated_client.Login("iggy", "iggy"));
    ASSERT_NO_THROW(unauthenticated_client.Disconnect());
    ASSERT_THROW(unauthenticated_client.GetTopics(iggy::Identifier::String(stream_name)), std::exception);
}

TEST_F(E2E_Topic, GetTopicsReturnsEmptyForStreamWithoutTopics) {
    RecordProperty("description", "Returns an empty topic list for an existing stream that has no topics.");
    const std::string stream_name = GetRandomName();

    auto client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);

    ASSERT_NO_THROW({
        const auto topics = client.GetTopics(iggy::Identifier::String(stream_name));
        EXPECT_TRUE(topics.empty());
    });
}

TEST_F(E2E_Topic, GetTopicsAfterTopicDeletionReturnsRemainingTopics) {
    RecordProperty("description", "Returns only non-deleted topics after a topic is deleted from the stream.");
    const std::string stream_name     = GetRandomName();
    const std::string deleted_topic   = GetRandomName();
    const std::string remaining_topic = GetRandomName();

    auto client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), deleted_topic,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), remaining_topic,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    ASSERT_NO_THROW(client.DeleteTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(deleted_topic)));

    ASSERT_NO_THROW({
        const auto topics = client.GetTopics(iggy::Identifier::String(stream_name));
        ASSERT_EQ(topics.size(), 1u);
        EXPECT_EQ(topics.front().Name(), remaining_topic);
    });
}

TEST_F(E2E_Topic, GetTopicsAfterTopicUpdateReturnsUpdatedInputFields) {
    RecordProperty("description", "Returns updated user-provided topic fields after a topic update.");
    const std::string stream_name        = GetRandomName();
    const std::string original_topic     = GetRandomName();
    const std::string updated_topic_name = GetRandomName();

    auto client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), original_topic,
                                       iggy::TopicCreateOptions().SetPartitionsCount(2)));
    ASSERT_NO_THROW(client.UpdateTopic(
        iggy::Identifier::String(stream_name), iggy::Identifier::String(original_topic), updated_topic_name,
        iggy::TopicUpdateOptions()
            .SetCompressionAlgorithm(iggy::CompressionAlgorithm::Gzip())
            .SetMessageExpiry(iggy::Expiry::Duration(1000))
            .SetMaxTopicSize(iggy::MaxTopicSize::FromBytes(1024ULL * 1024ULL * 1024ULL))));

    ASSERT_NO_THROW({
        const auto topics = client.GetTopics(iggy::Identifier::String(stream_name));
        ASSERT_EQ(topics.size(), 1u);
        EXPECT_EQ(topics.front().Name(), updated_topic_name);
        EXPECT_EQ(topics.front().PartitionsCount(), 2u);
        EXPECT_EQ(topics.front().CompressionAlgorithm(), "gzip");
        EXPECT_EQ(topics.front().MessageExpiry(), 1000u);
        EXPECT_EQ(topics.front().MaxTopicSize(), 1024ULL * 1024ULL * 1024ULL);
    });
}

TEST_F(E2E_Topic, UpdateTopicWorksCorrectly) {
    RecordProperty("description", "Returns a topic summary that matches topic details after updating a topic.");
    const std::string stream_name        = GetRandomName();
    const std::string original_topic     = GetRandomName();
    const std::string updated_topic_name = GetRandomName();

    auto client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), original_topic,
                                       iggy::TopicCreateOptions().SetPartitionsCount(2)));
    ASSERT_NO_THROW(client.UpdateTopic(
        iggy::Identifier::String(stream_name), iggy::Identifier::String(original_topic), updated_topic_name,
        iggy::TopicUpdateOptions()
            .SetCompressionAlgorithm(iggy::CompressionAlgorithm::Gzip())
            .SetMessageExpiry(iggy::Expiry::Duration(1000))
            .SetMaxTopicSize(iggy::MaxTopicSize::FromBytes(1024ULL * 1024ULL * 1024ULL))));

    ASSERT_NO_THROW({
        const auto topic_details =
            client.GetTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(updated_topic_name));
        const auto topics = client.GetTopics(iggy::Identifier::String(stream_name));
        ASSERT_EQ(topics.size(), 1u);

        const auto &topic_summary = topics.front();
        EXPECT_EQ(topic_summary.Id(), topic_details.Id());
        EXPECT_EQ(topic_summary.CreatedAt(), topic_details.CreatedAt());
        EXPECT_EQ(topic_summary.Name(), topic_details.Name());
        EXPECT_EQ(topic_summary.SizeBytes(), topic_details.SizeBytes());
        EXPECT_EQ(topic_summary.MessageExpiry(), topic_details.MessageExpiry());
        EXPECT_EQ(topic_summary.CompressionAlgorithm(), topic_details.CompressionAlgorithm());
        EXPECT_EQ(topic_summary.MaxTopicSize(), topic_details.MaxTopicSize());
        EXPECT_EQ(topic_summary.MessagesCount(), topic_details.MessagesCount());
        EXPECT_EQ(topic_summary.PartitionsCount(), topic_details.PartitionsCount());
    });
}

TEST_F(E2E_Topic, UpdateTopicDoesNotChangePartitionsCount) {
    RecordProperty("description", "Preserves the topic partition count after updating topic metadata.");
    const std::string stream_name            = GetRandomName();
    const std::string original_topic         = GetRandomName();
    const std::string updated_topic_name     = GetRandomName();
    constexpr std::uint32_t partitions_count = 3;

    auto client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), original_topic,
                                       iggy::TopicCreateOptions().SetPartitionsCount(partitions_count)));

    ASSERT_NO_THROW(client.UpdateTopic(
        iggy::Identifier::String(stream_name), iggy::Identifier::String(original_topic), updated_topic_name,
        iggy::TopicUpdateOptions()
            .SetCompressionAlgorithm(iggy::CompressionAlgorithm::Gzip())
            .SetMessageExpiry(iggy::Expiry::Duration(1000))
            .SetMaxTopicSize(iggy::MaxTopicSize::FromBytes(1024ULL * 1024ULL * 1024ULL))));

    ASSERT_NO_THROW({
        const auto topic_details =
            client.GetTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(updated_topic_name));
        EXPECT_EQ(topic_details.PartitionsCount(), partitions_count);
        EXPECT_EQ(topic_details.Partitions().size(), partitions_count);
    });
}

TEST_F(E2E_Topic, UpdateTopicDoesNotChangeMessages) {
    RecordProperty("description", "Keeps existing messages readable after updating topic metadata.");
    const std::string stream_name        = GetRandomName();
    const std::string original_topic     = GetRandomName();
    const std::string updated_topic_name = GetRandomName();

    iggy::ffi::Client *message_client = GetLoggedInClient();

    auto client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), original_topic,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));

    const auto created_stream = client.GetStream(iggy::Identifier::String(stream_name));
    ASSERT_EQ(created_stream.Topics().size(), 1u);
    const auto topic_id = created_stream.Topics().front().Id();

    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    messages.push_back(
        iggy::ffi::make_message(to_payload("message-before-topic-update"), rust::Vec<iggy::ffi::HeaderEntry>()));
    ASSERT_NO_THROW(message_client->send_messages(make_numeric_identifier(created_stream.Id()),
                                                  make_numeric_identifier(topic_id), "partition_id",
                                                  partition_id_bytes(0), std::move(messages)));

    ASSERT_NO_THROW(client.UpdateTopic(
        iggy::Identifier::String(stream_name), iggy::Identifier::String(original_topic), updated_topic_name,
        iggy::TopicUpdateOptions()
            .SetCompressionAlgorithm(iggy::CompressionAlgorithm::Gzip())
            .SetMessageExpiry(iggy::Expiry::Duration(1000))
            .SetMaxTopicSize(iggy::MaxTopicSize::FromBytes(1024ULL * 1024ULL * 1024ULL))));

    ASSERT_NO_THROW({
        const auto polled = message_client->poll_messages(make_numeric_identifier(created_stream.Id()),
                                                          make_string_identifier(updated_topic_name), 0, "consumer",
                                                          make_numeric_identifier(1), "offset", 0, 10, false);
        ASSERT_EQ(polled.count, 1u);
        ASSERT_EQ(polled.messages.size(), 1u);
        const std::string actual(polled.messages[0].payload.begin(), polled.messages[0].payload.end());
        EXPECT_EQ(actual, "message-before-topic-update");
    });
}

TEST_F(E2E_Topic, UpdateTopicWithAllOptionCombinationsUpdatesInputFields) {
    RecordProperty("description",
                   "Updates a topic across supported option combinations and verifies deterministic updated fields.");
    const std::string stream_name = GetRandomName();
    std::string topic_name        = GetRandomName();

    struct CompressionOption {
        std::string name;
        iggy::CompressionAlgorithm value;
    };
    const std::vector<CompressionOption> compression_algorithms = {
        {"none", iggy::CompressionAlgorithm::None()},
        {"gzip", iggy::CompressionAlgorithm::Gzip()},
    };
    struct ExpiryOption {
        std::string name;
        iggy::Expiry value;
    };
    const std::vector<ExpiryOption> expiry_options = {
        {"server_default", iggy::Expiry::ServerDefault()},
        {"never_expire", iggy::Expiry::NeverExpire()},
        {"duration", iggy::Expiry::Duration(1000)},
    };
    struct MaxTopicSizeOption {
        std::string name;
        iggy::MaxTopicSize value;
    };
    const std::vector<MaxTopicSizeOption> max_topic_sizes = {
        {"server_default", iggy::MaxTopicSize::ServerDefault()},
        {"unlimited", iggy::MaxTopicSize::Unlimited()},
        {"1GiB", iggy::MaxTopicSize::FromBytes(1024ULL * 1024ULL * 1024ULL)},
    };

    auto client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(2)));

    for (const auto &compression_algorithm : compression_algorithms) {
        for (const auto &expiry_option : expiry_options) {
            for (const auto &max_topic_size : max_topic_sizes) {
                const std::string updated_topic_name = GetRandomName();
                SCOPED_TRACE("compression=" + compression_algorithm.name + ", expiry_kind=" + expiry_option.name +
                             ", max_topic_size=" + max_topic_size.name);

                ASSERT_NO_THROW(client.UpdateTopic(iggy::Identifier::String(stream_name),
                                                   iggy::Identifier::String(topic_name), updated_topic_name,
                                                   iggy::TopicUpdateOptions()
                                                       .SetCompressionAlgorithm(compression_algorithm.value)
                                                       .SetMessageExpiry(expiry_option.value)
                                                       .SetMaxTopicSize(max_topic_size.value)));
                topic_name = updated_topic_name;
            }
        }
    }
}

TEST_F(E2E_Topic, UpdateTopicWithSameOptionsIsIdempotent) {
    RecordProperty("description", "Calling update_topic twice with the same options returns the same topic details.");
    const std::string stream_name        = GetRandomName();
    const std::string original_topic     = GetRandomName();
    const std::string updated_topic_name = GetRandomName();

    auto client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), original_topic,
                                       iggy::TopicCreateOptions().SetPartitionsCount(2)));

    const auto created_topic =
        client.GetTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(original_topic));

    ASSERT_NO_THROW(client.UpdateTopic(
        iggy::Identifier::String(stream_name), iggy::Identifier::Numeric(created_topic.Id()), updated_topic_name,
        iggy::TopicUpdateOptions()
            .SetCompressionAlgorithm(iggy::CompressionAlgorithm::Gzip())
            .SetMessageExpiry(iggy::Expiry::Duration(1000))
            .SetMaxTopicSize(iggy::MaxTopicSize::FromBytes(1024ULL * 1024ULL * 1024ULL))));
    const auto first_update =
        client.GetTopic(iggy::Identifier::String(stream_name), iggy::Identifier::Numeric(created_topic.Id()));

    ASSERT_NO_THROW(client.UpdateTopic(
        iggy::Identifier::String(stream_name), iggy::Identifier::Numeric(created_topic.Id()), updated_topic_name,
        iggy::TopicUpdateOptions()
            .SetCompressionAlgorithm(iggy::CompressionAlgorithm::Gzip())
            .SetMessageExpiry(iggy::Expiry::Duration(1000))
            .SetMaxTopicSize(iggy::MaxTopicSize::FromBytes(1024ULL * 1024ULL * 1024ULL))));
    const auto second_update =
        client.GetTopic(iggy::Identifier::String(stream_name), iggy::Identifier::Numeric(created_topic.Id()));

    EXPECT_EQ(second_update.Id(), first_update.Id());
    EXPECT_EQ(second_update.Name(), first_update.Name());
    EXPECT_EQ(second_update.PartitionsCount(), first_update.PartitionsCount());
    EXPECT_EQ(second_update.CompressionAlgorithm(), first_update.CompressionAlgorithm());
    EXPECT_EQ(second_update.MessageExpiry(), first_update.MessageExpiry());
    EXPECT_EQ(second_update.MaxTopicSize(), first_update.MaxTopicSize());
}

TEST_F(E2E_Topic, UpdateTopicWithDuplicateTopicNameThrows) {
    RecordProperty("description", "Rejects renaming a topic to another topic's existing name.");
    const std::string stream_name       = GetRandomName();
    const std::string first_topic_name  = GetRandomName();
    const std::string second_topic_name = GetRandomName();

    auto client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), first_topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), second_topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));

    ASSERT_THROW(client.UpdateTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(first_topic_name),
                                    second_topic_name,
                                    iggy::TopicUpdateOptions()
                                        .SetCompressionAlgorithm(iggy::CompressionAlgorithm::Gzip())
                                        .SetMessageExpiry(iggy::Expiry::Duration(1000))
                                        .SetMaxTopicSize(iggy::MaxTopicSize::FromBytes(1024ULL * 1024ULL * 1024ULL))),
                 std::exception);
}

TEST_F(E2E_Topic, UpdateTopicWithInvalidNamesThrows) {
    RecordProperty("description", "Rejects invalid topic names when updating a topic.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    auto client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));

    const std::vector<std::string> invalid_topic_names = {
        "",
        std::string(256, 'b'),
    };

    for (const auto &invalid_topic_name : invalid_topic_names) {
        SCOPED_TRACE("invalid_topic_name_length=" + std::to_string(invalid_topic_name.size()));

        ASSERT_THROW(
            client.UpdateTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name),
                               invalid_topic_name,
                               iggy::TopicUpdateOptions()
                                   .SetCompressionAlgorithm(iggy::CompressionAlgorithm::Gzip())
                                   .SetMessageExpiry(iggy::Expiry::Duration(1000))
                                   .SetMaxTopicSize(iggy::MaxTopicSize::FromBytes(1024ULL * 1024ULL * 1024ULL))),
            std::exception);
    }
}

TEST_F(E2E_Topic, UpdateTopicFailedValidationDoesNotMutateTopic) {
    RecordProperty("description", "Keeps the topic unchanged when update_topic fails wrapper validation.");
    const std::string stream_name        = GetRandomName();
    const std::string topic_name         = GetRandomName();
    const std::string updated_topic_name = GetRandomName();

    auto client = GetLoggedInHighLevelClient();

    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(
        client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                           iggy::TopicCreateOptions()
                               .SetPartitionsCount(2)
                               .SetCompressionAlgorithm(iggy::CompressionAlgorithm::Gzip())
                               .SetMessageExpiry(iggy::Expiry::Duration(1000))
                               .SetMaxTopicSize(iggy::MaxTopicSize::FromBytes(1024ULL * 1024ULL * 1024ULL))));

    const auto topic_before_update =
        client.GetTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name));

    ASSERT_THROW(client.UpdateTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name),
                                    updated_topic_name,
                                    iggy::TopicUpdateOptions()
                                        .SetCompressionAlgorithm(iggy::CompressionAlgorithm::None())
                                        .SetMessageExpiry(iggy::Expiry::Duration(2000))
                                        .SetMaxTopicSize(iggy::MaxTopicSize::ServerDefault())
                                        .SetRawEntries({{"max_topic_size", "not-a-size"}})),
                 std::exception);

    const auto topic_after_failed_update =
        client.GetTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name));

    EXPECT_EQ(topic_after_failed_update.Id(), topic_before_update.Id());
    EXPECT_EQ(topic_after_failed_update.Name(), topic_before_update.Name());
    EXPECT_EQ(topic_after_failed_update.PartitionsCount(), topic_before_update.PartitionsCount());
    EXPECT_EQ(topic_after_failed_update.CompressionAlgorithm(), topic_before_update.CompressionAlgorithm());
    EXPECT_EQ(topic_after_failed_update.MessageExpiry(), topic_before_update.MessageExpiry());
    EXPECT_EQ(topic_after_failed_update.MaxTopicSize(), topic_before_update.MaxTopicSize());

    EXPECT_THROW(client.GetTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(updated_topic_name)),
                 std::exception);
}

TEST_F(E2E_Topic, UpdateTopicBeforeLoginThrows) {
    RecordProperty("description", "Rejects update_topic before connect, and after connect but before login.");
    const std::string stream_name        = GetRandomName();
    const std::string topic_name         = GetRandomName();
    const std::string updated_topic_name = GetRandomName();

    auto client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));

    auto unauthenticated_client = GetLoggedOutHighLevelClient();

    ASSERT_THROW(unauthenticated_client.UpdateTopic(iggy::Identifier::String(stream_name),
                                                    iggy::Identifier::String(topic_name), updated_topic_name),
                 std::exception);
    ASSERT_NO_THROW(unauthenticated_client.Connect());
    ASSERT_THROW(unauthenticated_client.UpdateTopic(iggy::Identifier::String(stream_name),
                                                    iggy::Identifier::String(topic_name), updated_topic_name),
                 std::exception);
    ASSERT_NO_THROW(unauthenticated_client.Login("iggy", "iggy"));
    ASSERT_NO_THROW(unauthenticated_client.Disconnect());
    ASSERT_THROW(unauthenticated_client.UpdateTopic(iggy::Identifier::String(stream_name),
                                                    iggy::Identifier::String(topic_name), updated_topic_name),
                 std::exception);
}

TEST_F(E2E_Topic, UpdateTopicOnNonExistentStreamThrows) {
    RecordProperty("description", "Throws when updating a topic on a stream that does not exist.");
    const std::string stream_name        = GetRandomName();
    const std::string topic_name         = GetRandomName();
    const std::string updated_topic_name = GetRandomName();

    auto client = GetLoggedInHighLevelClient();
    ASSERT_THROW(client.UpdateTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name),
                                    updated_topic_name,
                                    iggy::TopicUpdateOptions()
                                        .SetCompressionAlgorithm(iggy::CompressionAlgorithm::Gzip())
                                        .SetMessageExpiry(iggy::Expiry::Duration(1000))
                                        .SetMaxTopicSize(iggy::MaxTopicSize::FromBytes(1024ULL * 1024ULL * 1024ULL))),
                 std::exception);
}

TEST_F(E2E_Topic, UpdateTopicOnNonExistentTopicThrows) {
    RecordProperty("description", "Throws when updating a topic that does not exist.");
    const std::string stream_name        = GetRandomName();
    const std::string topic_name         = GetRandomName();
    const std::string updated_topic_name = GetRandomName();

    auto client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);

    ASSERT_THROW(client.UpdateTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name),
                                    updated_topic_name,
                                    iggy::TopicUpdateOptions()
                                        .SetCompressionAlgorithm(iggy::CompressionAlgorithm::Gzip())
                                        .SetMessageExpiry(iggy::Expiry::Duration(1000))
                                        .SetMaxTopicSize(iggy::MaxTopicSize::FromBytes(1024ULL * 1024ULL * 1024ULL))),
                 std::exception);
}

TEST_F(E2E_Topic, GetTopicsAfterStreamDeletionReturnsEmpty) {
    RecordProperty("description", "Returns an empty topic list after deleting the stream that owned the topic.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    auto client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    ASSERT_NO_THROW(client.DeleteStream(iggy::Identifier::String(stream_name)));
    ForgetTrackedStream(stream_name);

    ASSERT_NO_THROW({
        const auto topics = client.GetTopics(iggy::Identifier::String(stream_name));
        EXPECT_TRUE(topics.empty());
    });
}

TEST_F(E2E_Topic, PurgeTopicOnNonExistentStreamThrows) {
    RecordProperty("description", "Throws when purging a topic on a stream that does not exist.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    auto client = GetLoggedInHighLevelClient();
    ASSERT_THROW(client.PurgeTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name)),
                 std::exception);
}

TEST_F(E2E_Topic, PurgeTopicAfterStreamDeletionThrows) {
    RecordProperty("description", "Throws when purging a topic after its stream has been deleted.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    auto client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    ASSERT_NO_THROW(client.DeleteStream(iggy::Identifier::String(stream_name)));
    ForgetTrackedStream(stream_name);

    ASSERT_THROW(client.PurgeTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name)),
                 std::exception);
}

TEST_F(E2E_Topic, PurgeTopicOnNonExistentTopicThrows) {
    RecordProperty("description", "Throws when purging a topic that does not exist.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    auto client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);

    ASSERT_THROW(client.PurgeTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name)),
                 std::exception);
}

TEST_F(E2E_Topic, PurgeTopicPreservesTopicMetadata) {
    RecordProperty("description", "Preserves topic metadata after purging its messages.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    iggy::ffi::Client *message_client = GetLoggedInClient();

    auto client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(
        client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                           iggy::TopicCreateOptions()
                               .SetPartitionsCount(3)
                               .SetCompressionAlgorithm(iggy::CompressionAlgorithm::Gzip())
                               .SetMessageExpiry(iggy::Expiry::Duration(1000))
                               .SetMaxTopicSize(iggy::MaxTopicSize::FromBytes(1024ULL * 1024ULL * 1024ULL))));

    auto stream_before_purge = client.GetStream(iggy::Identifier::String(stream_name));
    ASSERT_EQ(stream_before_purge.Topics().size(), 1u);

    rust::Vec<iggy::ffi::IggyMessageToSend> messages;
    messages.push_back(
        iggy::ffi::make_message(to_payload("preserve-topic-metadata"), rust::Vec<iggy::ffi::HeaderEntry>()));
    ASSERT_NO_THROW(message_client->send_messages(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), "partition_id",
                                                  partition_id_bytes(1), std::move(messages)));

    stream_before_purge = client.GetStream(iggy::Identifier::String(stream_name));
    ASSERT_EQ(stream_before_purge.Topics().size(), 1u);
    const auto &topic_with_messages = stream_before_purge.Topics().front();
    EXPECT_GT(topic_with_messages.MessagesCount(), 0u);
    EXPECT_GT(topic_with_messages.SizeBytes(), 0u);

    ASSERT_NO_THROW(client.PurgeTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name)));

    const auto stream_after_purge = client.GetStream(iggy::Identifier::String(stream_name));
    ASSERT_EQ(stream_after_purge.Topics().size(), 1u);
    const auto &topic_after_purge = stream_after_purge.Topics().front();

    EXPECT_EQ(topic_after_purge.Id(), topic_with_messages.Id());
    EXPECT_EQ(topic_after_purge.CreatedAt(), topic_with_messages.CreatedAt());
    EXPECT_EQ(topic_after_purge.Name(), topic_with_messages.Name());
    EXPECT_EQ(topic_after_purge.MessageExpiry(), topic_with_messages.MessageExpiry());
    EXPECT_EQ(topic_after_purge.CompressionAlgorithm(), topic_with_messages.CompressionAlgorithm());
    EXPECT_EQ(topic_after_purge.MaxTopicSize(), topic_with_messages.MaxTopicSize());
    EXPECT_EQ(topic_after_purge.PartitionsCount(), topic_with_messages.PartitionsCount());
}

TEST_F(E2E_Topic, PurgeTopicRemovesOnlyTargetTopicMessages) {
    RecordProperty("description", "Purges one topic's messages without affecting the other topics in the stream.");
    const std::string stream_name       = GetRandomName();
    const std::string first_topic_name  = GetRandomName();
    const std::string second_topic_name = GetRandomName();

    iggy::ffi::Client *message_client = GetLoggedInClient();

    auto client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), first_topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), second_topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));

    rust::Vec<iggy::ffi::IggyMessageToSend> first_topic_messages;
    for (std::uint32_t i = 0; i < 3; ++i) {
        first_topic_messages.push_back(iggy::ffi::make_message(to_payload("purge-topic-first-" + std::to_string(i)),
                                                               rust::Vec<iggy::ffi::HeaderEntry>()));
    }
    ASSERT_NO_THROW(message_client->send_messages(make_string_identifier(stream_name),
                                                  make_string_identifier(first_topic_name), "partition_id",
                                                  partition_id_bytes(0), std::move(first_topic_messages)));

    rust::Vec<iggy::ffi::IggyMessageToSend> second_topic_messages;
    for (std::uint32_t i = 0; i < 2; ++i) {
        second_topic_messages.push_back(iggy::ffi::make_message(to_payload("purge-topic-second-" + std::to_string(i)),
                                                                rust::Vec<iggy::ffi::HeaderEntry>()));
    }
    ASSERT_NO_THROW(message_client->send_messages(make_string_identifier(stream_name),
                                                  make_string_identifier(second_topic_name), "partition_id",
                                                  partition_id_bytes(0), std::move(second_topic_messages)));

    const auto stream_before_purge = client.GetStream(iggy::Identifier::String(stream_name));
    EXPECT_EQ(stream_before_purge.MessagesCount(), 5u);
    EXPECT_GT(stream_before_purge.SizeBytes(), 0u);

    std::unordered_map<std::string, std::uint64_t> messages_before_purge;
    for (const auto &topic : stream_before_purge.Topics()) {
        messages_before_purge[topic.Name()] = topic.MessagesCount();
    }
    EXPECT_EQ(messages_before_purge[first_topic_name], 3u);
    EXPECT_EQ(messages_before_purge[second_topic_name], 2u);

    ASSERT_NO_THROW(
        client.PurgeTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(first_topic_name)));

    const auto stream_after_purge = client.GetStream(iggy::Identifier::String(stream_name));
    EXPECT_EQ(stream_after_purge.TopicsCount(), 2u);
    EXPECT_EQ(stream_after_purge.MessagesCount(), 2u);
    EXPECT_GT(stream_after_purge.SizeBytes(), 0u);
    EXPECT_LT(stream_after_purge.SizeBytes(), stream_before_purge.SizeBytes());

    std::unordered_map<std::string, std::uint64_t> messages_after_purge;
    std::unordered_map<std::string, std::uint64_t> sizes_after_purge;
    for (const auto &topic : stream_after_purge.Topics()) {
        messages_after_purge[topic.Name()] = topic.MessagesCount();
        sizes_after_purge[topic.Name()]    = topic.SizeBytes();
    }
    EXPECT_EQ(messages_after_purge[first_topic_name], 0u);
    EXPECT_EQ(sizes_after_purge[first_topic_name], 0u);
    EXPECT_EQ(messages_after_purge[second_topic_name], 2u);
    EXPECT_GT(sizes_after_purge[second_topic_name], 0u);
}

TEST_F(E2E_Topic, PurgeTopicAcrossMultiplePartitionsClearsAllPartitions) {
    RecordProperty("description", "Purges all messages from every partition in the topic.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    iggy::ffi::Client *message_client = GetLoggedInClient();

    auto client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(3)));

    for (std::uint32_t partition_id = 0; partition_id < 3; ++partition_id) {
        rust::Vec<iggy::ffi::IggyMessageToSend> messages;
        for (std::uint32_t i = 0; i < 2; ++i) {
            messages.push_back(iggy::ffi::make_message(
                to_payload("purge-topic-partition-" + std::to_string(partition_id) + "-" + std::to_string(i)),
                rust::Vec<iggy::ffi::HeaderEntry>()));
        }
        ASSERT_NO_THROW(message_client->send_messages(make_string_identifier(stream_name),
                                                      make_string_identifier(topic_name), "partition_id",
                                                      partition_id_bytes(partition_id), std::move(messages)));
    }

    const auto stream_before_purge = client.GetStream(iggy::Identifier::String(stream_name));
    EXPECT_EQ(stream_before_purge.MessagesCount(), 6u);
    ASSERT_EQ(stream_before_purge.Topics().size(), 1u);
    EXPECT_EQ(stream_before_purge.Topics().front().PartitionsCount(), 3u);
    EXPECT_EQ(stream_before_purge.Topics().front().MessagesCount(), 6u);

    ASSERT_NO_THROW(client.PurgeTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name)));

    const auto stream_after_purge = client.GetStream(iggy::Identifier::String(stream_name));
    EXPECT_EQ(stream_after_purge.MessagesCount(), 0u);
    ASSERT_EQ(stream_after_purge.Topics().size(), 1u);
    EXPECT_EQ(stream_after_purge.Topics().front().PartitionsCount(), 3u);
    EXPECT_EQ(stream_after_purge.Topics().front().MessagesCount(), 0u);
    EXPECT_EQ(stream_after_purge.Topics().front().SizeBytes(), 0u);
}

TEST_F(E2E_Topic, PurgeTopicThenSendMessagesAgainSucceeds) {
    RecordProperty("description", "Allows sending fresh messages again after purging the topic.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    iggy::ffi::Client *message_client = GetLoggedInClient();

    auto client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));

    rust::Vec<iggy::ffi::IggyMessageToSend> first_batch;
    first_batch.push_back(
        iggy::ffi::make_message(to_payload("before-topic-purge"), rust::Vec<iggy::ffi::HeaderEntry>()));
    ASSERT_NO_THROW(message_client->send_messages(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), "partition_id",
                                                  partition_id_bytes(0), std::move(first_batch)));

    ASSERT_NO_THROW(client.PurgeTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name)));

    rust::Vec<iggy::ffi::IggyMessageToSend> second_batch;
    second_batch.push_back(
        iggy::ffi::make_message(to_payload("after-topic-purge-0"), rust::Vec<iggy::ffi::HeaderEntry>()));
    second_batch.push_back(
        iggy::ffi::make_message(to_payload("after-topic-purge-1"), rust::Vec<iggy::ffi::HeaderEntry>()));
    ASSERT_NO_THROW(message_client->send_messages(make_string_identifier(stream_name),
                                                  make_string_identifier(topic_name), "partition_id",
                                                  partition_id_bytes(0), std::move(second_batch)));

    const auto stream_after_resend = client.GetStream(iggy::Identifier::String(stream_name));
    EXPECT_EQ(stream_after_resend.MessagesCount(), 2u);
    ASSERT_EQ(stream_after_resend.Topics().size(), 1u);
    EXPECT_EQ(stream_after_resend.Topics().front().MessagesCount(), 2u);
    EXPECT_GT(stream_after_resend.Topics().front().SizeBytes(), 0u);
}

TEST_F(E2E_Topic, PurgeTopicTwiceKeepsTargetTopicEmptyAndOtherTopicsUntouched) {
    RecordProperty("description",
                   "Allows purging the same topic twice and keeps the target topic empty without affecting siblings.");
    const std::string stream_name       = GetRandomName();
    const std::string first_topic_name  = GetRandomName();
    const std::string second_topic_name = GetRandomName();

    iggy::ffi::Client *message_client = GetLoggedInClient();

    auto client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), first_topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), second_topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));

    rust::Vec<iggy::ffi::IggyMessageToSend> first_topic_messages;
    for (std::uint32_t i = 0; i < 3; ++i) {
        first_topic_messages.push_back(iggy::ffi::make_message(
            to_payload("purge-topic-twice-first-" + std::to_string(i)), rust::Vec<iggy::ffi::HeaderEntry>()));
    }
    ASSERT_NO_THROW(message_client->send_messages(make_string_identifier(stream_name),
                                                  make_string_identifier(first_topic_name), "partition_id",
                                                  partition_id_bytes(0), std::move(first_topic_messages)));

    rust::Vec<iggy::ffi::IggyMessageToSend> second_topic_messages;
    for (std::uint32_t i = 0; i < 2; ++i) {
        second_topic_messages.push_back(iggy::ffi::make_message(
            to_payload("purge-topic-twice-second-" + std::to_string(i)), rust::Vec<iggy::ffi::HeaderEntry>()));
    }
    ASSERT_NO_THROW(message_client->send_messages(make_string_identifier(stream_name),
                                                  make_string_identifier(second_topic_name), "partition_id",
                                                  partition_id_bytes(0), std::move(second_topic_messages)));

    ASSERT_NO_THROW(
        client.PurgeTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(first_topic_name)));
    const auto stream_after_first_purge = client.GetStream(iggy::Identifier::String(stream_name));
    EXPECT_EQ(stream_after_first_purge.TopicsCount(), 2u);
    EXPECT_EQ(stream_after_first_purge.MessagesCount(), 2u);

    std::unordered_map<std::string, std::uint64_t> messages_after_first_purge;
    std::unordered_map<std::string, std::uint64_t> sizes_after_first_purge;
    for (const auto &topic : stream_after_first_purge.Topics()) {
        messages_after_first_purge[topic.Name()] = topic.MessagesCount();
        sizes_after_first_purge[topic.Name()]    = topic.SizeBytes();
    }
    EXPECT_EQ(messages_after_first_purge[first_topic_name], 0u);
    EXPECT_EQ(sizes_after_first_purge[first_topic_name], 0u);
    EXPECT_EQ(messages_after_first_purge[second_topic_name], 2u);
    EXPECT_GT(sizes_after_first_purge[second_topic_name], 0u);

    ASSERT_NO_THROW(
        client.PurgeTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(first_topic_name)));
    const auto stream_after_second_purge = client.GetStream(iggy::Identifier::String(stream_name));
    EXPECT_EQ(stream_after_second_purge.TopicsCount(), 2u);
    EXPECT_EQ(stream_after_second_purge.MessagesCount(), 2u);

    std::unordered_map<std::string, std::uint64_t> messages_after_second_purge;
    std::unordered_map<std::string, std::uint64_t> sizes_after_second_purge;
    for (const auto &topic : stream_after_second_purge.Topics()) {
        messages_after_second_purge[topic.Name()] = topic.MessagesCount();
        sizes_after_second_purge[topic.Name()]    = topic.SizeBytes();
    }
    EXPECT_EQ(messages_after_second_purge[first_topic_name], 0u);
    EXPECT_EQ(sizes_after_second_purge[first_topic_name], 0u);
    EXPECT_EQ(messages_after_second_purge[second_topic_name], 2u);
    EXPECT_GT(sizes_after_second_purge[second_topic_name], 0u);
}

TEST_F(E2E_Topic, PurgeTopicBeforeLoginThrows) {
    RecordProperty("description", "Throws when topic purge is attempted before authentication.");
    const std::string stream_name = GetRandomName();
    const std::string topic_name  = GetRandomName();

    auto client = GetLoggedInHighLevelClient();
    ASSERT_NO_THROW(client.CreateStream(stream_name));
    TrackStream(stream_name);
    ASSERT_NO_THROW(client.CreateTopic(iggy::Identifier::String(stream_name), topic_name,
                                       iggy::TopicCreateOptions().SetPartitionsCount(1)));

    auto unauthenticated_client = GetLoggedOutHighLevelClient();

    ASSERT_THROW(
        unauthenticated_client.PurgeTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name)),
        std::exception);
    ASSERT_NO_THROW(unauthenticated_client.Connect());
    ASSERT_THROW(
        unauthenticated_client.PurgeTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name)),
        std::exception);
    ASSERT_NO_THROW(unauthenticated_client.Login("iggy", "iggy"));
    ASSERT_NO_THROW(unauthenticated_client.Disconnect());
    ASSERT_THROW(
        unauthenticated_client.PurgeTopic(iggy::Identifier::String(stream_name), iggy::Identifier::String(topic_name)),
        std::exception);
}
