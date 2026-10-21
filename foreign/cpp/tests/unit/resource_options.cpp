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
#include <fstream>
#include <functional>
#include <map>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "iggy.hpp"

namespace {

constexpr char kFixturePath[] = "tests/fixtures/resource_options.json";

using SurfaceCheck = std::function<bool()>;
using Surface      = std::map<std::string, SurfaceCheck>;

struct ScopeSurface {
    Surface create;
    Surface update;
    Surface reset;
    Surface read_only;
};

const nlohmann::json &Fixture() {
    static const nlohmann::json fixture = [] {
        std::ifstream file(kFixturePath);
        if (!file) {
            throw std::runtime_error(std::string("Cannot open ") + kFixturePath);
        }
        return nlohmann::json::parse(file);
    }();
    return fixture;
}

std::vector<std::uint8_t> FromHex(const std::string &hex) {
    std::vector<std::uint8_t> bytes;
    bytes.reserve(hex.size() / 2);
    for (std::size_t index = 0; index + 1 < hex.size(); index += 2) {
        bytes.push_back(static_cast<std::uint8_t>(std::stoul(hex.substr(index, 2), nullptr, 16)));
    }
    return bytes;
}

// Walks the enum from zero until to_string refuses a value, so a member added
// to the public enum is checked without a second list here.
template <typename Key>
std::vector<std::string> PublicKeyNames() {
    std::vector<std::string> names;
    for (unsigned value = 0; value <= 255; ++value) {
        try {
            names.emplace_back(iggy::to_string(static_cast<Key>(value)));
        } catch (const std::invalid_argument &) {
            break;
        }
    }
    return names;
}

std::vector<std::string> DescribedKeys(const std::string &scope) {
    std::vector<std::string> keys;
    for (const auto &row : Fixture().at("catalog")) {
        const auto &scopes = row.at("scopes");
        if (row.at("described").get<bool>() &&
            std::find(scopes.begin(), scopes.end(), nlohmann::json(scope)) != scopes.end()) {
            keys.push_back(row.at("key").get<std::string>());
        }
    }
    return keys;
}

std::set<std::string> CatalogKeys(const std::string &scope, const std::function<bool(const nlohmann::json &)> &filter) {
    std::set<std::string> keys;
    for (const auto &key : DescribedKeys(scope)) {
        for (const auto &row : Fixture().at("catalog")) {
            if (row.at("key") == key && filter(row)) {
                keys.insert(key);
            }
        }
    }
    return keys;
}

std::set<std::string> SurfaceKeys(const Surface &surface) {
    std::set<std::string> keys;
    for (const auto &[key, check] : surface) {
        EXPECT_TRUE(check()) << key;
        keys.insert(key);
    }
    return keys;
}

template <typename Options, typename Key>
SurfaceCheck ResetMember(Key key) {
    return [key] {
        Options options;
        options.Reset(key);
        return options.ResetKeys().count(key) == 1;
    };
}

std::map<std::string, ScopeSurface> TypedSurface() {
    using iggy::StreamCreateOptions;
    using iggy::StreamUpdateOptions;
    using iggy::TopicCreateOptions;
    using iggy::TopicUpdateOptions;
    using iggy::UserUpdateOptions;
    return {
        {"topic",
         {{
              {"compression_algorithm",
               [] {
                   return TopicCreateOptions()
                       .SetCompressionAlgorithm(iggy::CompressionAlgorithm::Gzip())
                       .CompressionAlgorithm()
                       .has_value();
               }},
              {"message_expiry",
               [] {
                   return TopicCreateOptions()
                       .SetMessageExpiry(iggy::Expiry::NeverExpire())
                       .MessageExpiry()
                       .has_value();
               }},
              {"max_topic_size",
               [] {
                   return TopicCreateOptions()
                       .SetMaxTopicSize(iggy::MaxTopicSize::Unlimited())
                       .MaxTopicSize()
                       .has_value();
               }},
              {"segment_size", [] { return TopicCreateOptions().SetSegmentSize(1048576).SegmentSize().has_value(); }},
              {"durability",
               [] { return TopicCreateOptions().SetDurability(iggy::Durability::Persisted).Durability().has_value(); }},
              {"consumer_offset_durability",
               [] {
                   return TopicCreateOptions()
                       .SetConsumerOffsetDurability(iggy::Durability::Persisted)
                       .ConsumerOffsetDurability()
                       .has_value();
               }},
              {"partition_resize_policy",
               [] {
                   return TopicCreateOptions()
                       .SetPartitionResizePolicy(iggy::PartitionResizePolicy::Fixed)
                       .PartitionResizePolicy()
                       .has_value();
               }},
              {"messages_required_to_save",
               [] { return TopicCreateOptions().SetMessagesRequiredToSave(1).MessagesRequiredToSave().has_value(); }},
              {"size_of_messages_required_to_save",
               [] {
                   return TopicCreateOptions()
                       .SetSizeOfMessagesRequiredToSave(1)
                       .SizeOfMessagesRequiredToSave()
                       .has_value();
               }},
              {"preallocate_segments",
               [] { return TopicCreateOptions().SetPreallocateSegments(true).PreallocateSegments().has_value(); }},
          },
          {
              {"name", [] { return TopicUpdateOptions().SetName("renamed").Name().has_value(); }},
              {"message_expiry",
               [] {
                   return TopicUpdateOptions()
                       .SetMessageExpiry(iggy::Expiry::NeverExpire())
                       .MessageExpiry()
                       .has_value();
               }},
              {"max_topic_size",
               [] {
                   return TopicUpdateOptions()
                       .SetMaxTopicSize(iggy::MaxTopicSize::Unlimited())
                       .MaxTopicSize()
                       .has_value();
               }},
          },
          {
              {"message_expiry", ResetMember<TopicUpdateOptions>(iggy::TopicOptionKey::MessageExpiry)},
              {"max_topic_size", ResetMember<TopicUpdateOptions>(iggy::TopicOptionKey::MaxTopicSize)},
          },
          {}}},
        {"stream",
         {{
              {"encryption_key",
               [] {
                   return StreamCreateOptions().SetEncryptionKey(std::vector<std::uint8_t>(32, 7)).HasEncryptionKey();
               }},
          },
          {
              {"name", [] { return StreamUpdateOptions().SetName("renamed").Name().has_value(); }},
          },
          {},
          {
              {"encryption_key_source",
               [] {
                   static_cast<void>(&iggy::StreamDetails::EncryptionKeySource);
                   return true;
               }},
              {"encryption_key_fingerprint",
               [] {
                   static_cast<void>(&iggy::StreamDetails::EncryptionKeyFingerprint);
                   return true;
               }},
              {"encryption_kek_fingerprint",
               [] {
                   static_cast<void>(&iggy::StreamDetails::EncryptionKekFingerprints);
                   return true;
               }},
          }}},
        {"user",
         {{},
          {
              {"username", [] { return UserUpdateOptions().SetUsername("renamed").Username().has_value(); }},
              {"status", [] { return UserUpdateOptions().SetStatus(iggy::UserStatus::Inactive).Status().has_value(); }},
          },
          {},
          {}}},
    };
}

template <typename Operation>
::testing::AssertionResult ThrowsIggyError(Operation &&operation, const nlohmann::json &error) {
    const auto code = error.at("code").get<std::uint32_t>();
    const auto kind = error.at("name").get<std::string>();
    try {
        std::forward<Operation>(operation)();
    } catch (const iggy::IggyException &exception) {
        if (exception.Code() == code && exception.Kind() == kind) {
            return ::testing::AssertionSuccess();
        }
        return ::testing::AssertionFailure() << "expected " << kind << " (" << code << "), got '" << exception.what()
                                             << "' with kind '" << exception.Kind() << "'";
    }
    return ::testing::AssertionFailure() << "expected " << kind << " (" << code << "), nothing was thrown";
}

}  // namespace

TEST(ResourceOptionsFixtureTest, GivenFixtureKeyWhenFingerprintingShouldMatchSharedVector) {
    const auto &stream_key = Fixture().at("stream_key");
    const auto expected    = stream_key.at("fingerprint").get<std::string>();

    EXPECT_EQ(iggy::EncryptionKeyFingerprint(FromHex(stream_key.at("key_hex").get<std::string>())), expected);
    EXPECT_EQ(iggy::EncryptionKeyFingerprint(stream_key.at("key_base64").get<std::string>()), expected);
}

TEST(ResourceOptionsFixtureTest, GivenInvalidKeyVectorsWhenFingerprintingShouldRefuseWithoutKeyMaterial) {
    const auto &stream_key = Fixture().at("stream_key");
    const auto key_base64  = stream_key.at("key_base64").get<std::string>();
    for (const auto &vector : stream_key.at("invalid_keys")) {
        SCOPED_TRACE(vector.at("case").get<std::string>());
        const auto &input = vector.at("input");
        if (input.contains("raw_hex")) {
            const auto raw = FromHex(input.at("raw_hex").get<std::string>());
            EXPECT_TRUE(ThrowsIggyError([&raw] { iggy::EncryptionKeyFingerprint(raw); }, vector.at("error")));
        } else {
            const auto text = input.at("base64").get<std::string>();
            EXPECT_TRUE(ThrowsIggyError([&text] { iggy::EncryptionKeyFingerprint(text); }, vector.at("error")));
            try {
                iggy::EncryptionKeyFingerprint(text);
            } catch (const iggy::IggyException &error) {
                EXPECT_EQ(std::string(error.what()).find(key_base64.substr(0, 40)), std::string::npos);
            }
        }
    }
}

TEST(ResourceOptionsFixtureTest, GivenFixtureCatalogWhenListingPublicKeyEnumsShouldMatchDescribedKeysInOrder) {
    EXPECT_EQ(PublicKeyNames<iggy::StreamOptionKey>(), DescribedKeys("stream"));
    EXPECT_EQ(PublicKeyNames<iggy::TopicOptionKey>(), DescribedKeys("topic"));
    EXPECT_EQ(PublicKeyNames<iggy::UserOptionKey>(), DescribedKeys("user"));
}

TEST(ResourceOptionsFixtureTest, GivenFixtureCatalogWhenComparingTypedSurfaceShouldCoverEveryKeyByMutability) {
    const auto flags      = Fixture().at("constants").at("option_flags");
    const auto resettable = flags.at("resettable").get<std::uint32_t>();
    for (const auto &[scope, surface] : TypedSurface()) {
        SCOPED_TRACE(scope);
        EXPECT_EQ(SurfaceKeys(surface.create), CatalogKeys(scope, [](const nlohmann::json &row) {
                      return row.at("storage") == "map" &&
                             (row.at("mutability") == "create_only" || row.at("mutability") == "updatable");
                  }));
        EXPECT_EQ(SurfaceKeys(surface.update),
                  CatalogKeys(scope, [](const nlohmann::json &row) { return row.at("mutability") == "updatable"; }));
        EXPECT_EQ(SurfaceKeys(surface.reset), CatalogKeys(scope, [resettable](const nlohmann::json &row) {
                      return (row.at("flags").get<std::uint32_t>() & resettable) != 0;
                  }));
        EXPECT_EQ(SurfaceKeys(surface.read_only),
                  CatalogKeys(scope, [](const nlohmann::json &row) { return row.at("mutability") == "read_only"; }));
    }
}

TEST(ResourceOptionsFixtureTest, GivenRawResetNamesWhenBuildingUpdatesShouldKeepThemBesideTypedResets) {
    iggy::TopicUpdateOptions topic;
    topic.Reset(iggy::TopicOptionKey::MaxTopicSize).RawReset("future_key");
    EXPECT_EQ(topic.ResetKeys(), std::set<iggy::TopicOptionKey>{iggy::TopicOptionKey::MaxTopicSize});
    EXPECT_EQ(topic.RawResetKeys(), std::set<std::string>{"future_key"});

    iggy::StreamUpdateOptions stream;
    EXPECT_TRUE(stream.RawReset("future_key").ResetKeys().empty());
    EXPECT_EQ(stream.RawResetKeys(), std::set<std::string>{"future_key"});

    iggy::UserUpdateOptions user;
    EXPECT_EQ(user.RawReset("future_key").RawResetKeys(), std::set<std::string>{"future_key"});
}

TEST(ResourceOptionsFixtureTest, GivenReadmeSampleWhenCompilingShouldMatchThePublicApi) {
    // Never invoked, it only has to compile, so the README sample cannot drift from the API.
    const auto sample = [](iggy::IggyBlockingClient &client, const std::function<std::string()> &LoadStreamKey) {
        const std::string key_base64 = LoadStreamKey();
        const auto stream =
            client.CreateStream("payments", iggy::StreamCreateOptions().SetEncryptionKeyBase64(key_base64));
        const bool same_key = stream.EncryptionKeyFingerprint() == iggy::EncryptionKeyFingerprint(key_base64);

        const auto stream_id = iggy::Identifier::Numeric(stream.Id());
        const auto topic     = client.CreateTopic(stream_id, "orders",
                                                  iggy::TopicCreateOptions()
                                                      .SetPartitionsCount(3)
                                                      .SetDurability(iggy::Durability::Persisted)
                                                      .SetPartitionResizePolicy(iggy::PartitionResizePolicy::Fixed));
        client.UpdateTopic(stream_id, iggy::Identifier::Numeric(topic.Id()),
                           iggy::TopicUpdateOptions()
                               .SetMessageExpiry(iggy::Expiry::Duration(3600000000))
                               .Reset(iggy::TopicOptionKey::MaxTopicSize));
        return same_key;
    };
    static_cast<void>(sample);
}
