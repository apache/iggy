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
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <functional>
#include <limits>
#include <map>
#include <optional>
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

template <typename Holder>
using DurationGetter = std::optional<std::chrono::microseconds> (Holder::*)() const;

template <typename Options>
using DurationSetter = Options &(Options::*)(std::chrono::microseconds);

// The durations of a geo topic by catalog key, on the options that set them.
template <typename Options>
const std::map<std::string, std::pair<DurationSetter<Options>, DurationGetter<Options>>> &GeoDurationAccessors() {
    static const std::map<std::string, std::pair<DurationSetter<Options>, DurationGetter<Options>>> accessors = {
        {"fresh_upload_interval", {&Options::SetFreshUploadInterval, &Options::FreshUploadInterval}},
        {"consumer_offset_upload_interval",
         {&Options::SetConsumerOffsetUploadInterval, &Options::ConsumerOffsetUploadInterval}},
        {"positions_publish_interval", {&Options::SetPositionsPublishInterval, &Options::PositionsPublishInterval}},
        {"tombstone_grace", {&Options::SetTombstoneGrace, &Options::TombstoneGrace}},
        {"positions_resume_timeout", {&Options::SetPositionsResumeTimeout, &Options::PositionsResumeTimeout}},
        {"long_term_rewrite_max_interval",
         {&Options::SetLongTermRewriteMaxInterval, &Options::LongTermRewriteMaxInterval}},
    };
    return accessors;
}

// The durations of a geo topic by catalog key, on the topics that read them.
template <typename Topic>
const std::map<std::string, DurationGetter<Topic>> &GeoDurationGetters() {
    static const std::map<std::string, DurationGetter<Topic>> getters = {
        {"fresh_upload_interval", &Topic::FreshUploadInterval},
        {"consumer_offset_upload_interval", &Topic::ConsumerOffsetUploadInterval},
        {"positions_publish_interval", &Topic::PositionsPublishInterval},
        {"tombstone_grace", &Topic::TombstoneGrace},
        {"positions_resume_timeout", &Topic::PositionsResumeTimeout},
        {"long_term_rewrite_max_interval", &Topic::LongTermRewriteMaxInterval},
    };
    return getters;
}

using FfiGeoDurationField =
    std::pair<bool iggy::ffi::GeoTopicDurations::*, std::uint64_t iggy::ffi::GeoTopicDurations::*>;

// The bridge fields of each duration, by catalog key.
const std::map<std::string, FfiGeoDurationField> &FfiGeoDurationFields() {
    using Durations                                                = iggy::ffi::GeoTopicDurations;
    static const std::map<std::string, FfiGeoDurationField> fields = {
        {"fresh_upload_interval", {&Durations::has_fresh_upload_interval, &Durations::fresh_upload_interval_micros}},
        {"consumer_offset_upload_interval",
         {&Durations::has_consumer_offset_upload_interval, &Durations::consumer_offset_upload_interval_micros}},
        {"positions_publish_interval",
         {&Durations::has_positions_publish_interval, &Durations::positions_publish_interval_micros}},
        {"tombstone_grace", {&Durations::has_tombstone_grace, &Durations::tombstone_grace_micros}},
        {"positions_resume_timeout",
         {&Durations::has_positions_resume_timeout, &Durations::positions_resume_timeout_micros}},
        {"long_term_rewrite_max_interval",
         {&Durations::has_long_term_rewrite_max_interval, &Durations::long_term_rewrite_max_interval_micros}},
    };
    return fields;
}

// A zero duration is a value, so setting one must read back as set.
template <typename Options>
SurfaceCheck GeoDurationMember(const std::string &key) {
    return [key] {
        const auto &[set, get] = GeoDurationAccessors<Options>().at(key);
        Options options;
        (options.*set)(std::chrono::microseconds::zero());
        return (options.*get)() == std::chrono::microseconds::zero();
    };
}

// A projection or input lists a duration as decimal microseconds, and leaves out one that is unset.
std::optional<std::chrono::microseconds> FixtureMicros(const nlohmann::json &object, const std::string &key) {
    if (!object.contains(key)) {
        return std::nullopt;
    }
    return std::chrono::microseconds(std::stoll(object.at(key).get<std::string>()));
}

const nlohmann::json &FixtureCase(const std::string &section, const std::string &name) {
    for (const auto &vector : Fixture().at(section)) {
        if (vector.at("case") == name) {
            return vector;
        }
    }
    throw std::runtime_error("No fixture case " + section + "." + name);
}

// Sets every duration the input names through the typed setters, and checks each reads back.
template <typename Options>
void ExpectTypedGeoDurations(const nlohmann::json &input) {
    Options options;
    for (const auto &[key, accessors] : GeoDurationAccessors<Options>()) {
        ASSERT_TRUE(input.contains(key)) << key;
        (options.*accessors.first)(*FixtureMicros(input, key));
    }
    for (const auto &[key, accessors] : GeoDurationAccessors<Options>()) {
        EXPECT_EQ((options.*accessors.second)(), FixtureMicros(input, key)) << key;
    }
}

// The bridge durations of a topic response projection.
iggy::ffi::GeoTopicDurations FfiGeoDurations(const nlohmann::json &expected) {
    iggy::ffi::GeoTopicDurations durations{};
    for (const auto &[key, fields] : FfiGeoDurationFields()) {
        if (const auto micros = FixtureMicros(expected, key)) {
            durations.*fields.first  = true;
            durations.*fields.second = static_cast<std::uint64_t>(micros->count());
        }
    }
    return durations;
}

template <typename Holder>
using BytesGetter = std::optional<std::uint64_t> (Holder::*)() const;

template <typename Options>
using BytesSetter = Options &(Options::*)(std::uint64_t);

// The read cache settings of a topic by catalog key, on the options that set them.
template <typename Options>
const std::map<std::string, std::pair<BytesSetter<Options>, BytesGetter<Options>>> &ReadCacheAccessors() {
    static const std::map<std::string, std::pair<BytesSetter<Options>, BytesGetter<Options>>> accessors = {
        {"read_cache_size", {&Options::SetReadCacheSize, &Options::ReadCacheSize}},
        {"read_ahead", {&Options::SetReadAhead, &Options::ReadAhead}},
    };
    return accessors;
}

// The read cache settings of a topic by catalog key, on the topics that read them.
template <typename Topic>
const std::map<std::string, BytesGetter<Topic>> &ReadCacheGetters() {
    static const std::map<std::string, BytesGetter<Topic>> getters = {
        {"read_cache_size", &Topic::ReadCacheSize},
        {"read_ahead", &Topic::ReadAhead},
    };
    return getters;
}

using FfiReadCacheField = std::pair<bool iggy::ffi::TopicReadCache::*, std::uint64_t iggy::ffi::TopicReadCache::*>;

// The bridge fields of each read cache setting, by catalog key.
const std::map<std::string, FfiReadCacheField> &FfiReadCacheFields() {
    using ReadCache                                              = iggy::ffi::TopicReadCache;
    static const std::map<std::string, FfiReadCacheField> fields = {
        {"read_cache_size", {&ReadCache::has_read_cache_size, &ReadCache::read_cache_size}},
        {"read_ahead", {&ReadCache::has_read_ahead, &ReadCache::read_ahead}},
    };
    return fields;
}

// Zero turns a read cache setting off, so setting it must read back as set.
template <typename Options>
SurfaceCheck ReadCacheMember(const std::string &key) {
    return [key] {
        const auto &[set, get] = ReadCacheAccessors<Options>().at(key);
        Options options;
        (options.*set)(0);
        return (options.*get)() == std::uint64_t{0};
    };
}

// A projection or input lists a read cache setting as decimal bytes, and leaves out one that is unset.
std::optional<std::uint64_t> FixtureBytes(const nlohmann::json &object, const std::string &key) {
    if (!object.contains(key)) {
        return std::nullopt;
    }
    return std::stoull(object.at(key).get<std::string>());
}

// Sets every read cache setting the input names through the typed setters, and checks each reads back.
template <typename Options>
void ExpectTypedReadCache(const nlohmann::json &input) {
    Options options;
    for (const auto &[key, accessors] : ReadCacheAccessors<Options>()) {
        ASSERT_TRUE(input.contains(key)) << key;
        (options.*accessors.first)(*FixtureBytes(input, key));
    }
    for (const auto &[key, accessors] : ReadCacheAccessors<Options>()) {
        EXPECT_EQ((options.*accessors.second)(), FixtureBytes(input, key)) << key;
    }
}

// The bridge read cache of a topic response projection.
iggy::ffi::TopicReadCache FfiReadCache(const nlohmann::json &expected) {
    iggy::ffi::TopicReadCache read_cache{};
    for (const auto &[key, fields] : FfiReadCacheFields()) {
        if (const auto bytes = FixtureBytes(expected, key)) {
            read_cache.*fields.first  = true;
            read_cache.*fields.second = *bytes;
        }
    }
    return read_cache;
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
              {"replication_factor",
               [] { return TopicCreateOptions().SetReplicationFactor(1).ReplicationFactor().has_value(); }},
              {"segment_max_age",
               [] {
                   return TopicCreateOptions()
                       .SetSegmentMaxAge(iggy::Expiry::NeverExpire())
                       .SegmentMaxAge()
                       .has_value();
               }},
              {"tiered_storage",
               [] { return TopicCreateOptions().SetTieredStorage(true).TieredStorage().has_value(); }},
              {"local_retention_age",
               [] {
                   return TopicCreateOptions()
                       .SetLocalRetentionAge(iggy::Expiry::NeverExpire())
                       .LocalRetentionAge()
                       .has_value();
               }},
              {"local_retention_size",
               [] {
                   return TopicCreateOptions()
                       .SetLocalRetentionSize(iggy::MaxTopicSize::Unlimited())
                       .LocalRetentionSize()
                       .has_value();
               }},
              {"topic_type",
               [] { return TopicCreateOptions().SetTopicType(iggy::TopicType::Geo).TopicType().has_value(); }},
              {"geo_mode",
               [] { return TopicCreateOptions().SetGeoMode(iggy::GeoMode::GeoTiered).GeoMode().has_value(); }},
              {"fresh_upload_interval", GeoDurationMember<TopicCreateOptions>("fresh_upload_interval")},
              {"consumer_offset_upload_interval",
               GeoDurationMember<TopicCreateOptions>("consumer_offset_upload_interval")},
              {"positions_publish_interval", GeoDurationMember<TopicCreateOptions>("positions_publish_interval")},
              {"tombstone_grace", GeoDurationMember<TopicCreateOptions>("tombstone_grace")},
              {"positions_resume_timeout", GeoDurationMember<TopicCreateOptions>("positions_resume_timeout")},
              {"long_term_rewrite_max_interval",
               GeoDurationMember<TopicCreateOptions>("long_term_rewrite_max_interval")},
              {"read_cache_size", ReadCacheMember<TopicCreateOptions>("read_cache_size")},
              {"read_ahead", ReadCacheMember<TopicCreateOptions>("read_ahead")},
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
              {"tiered_storage",
               [] { return TopicUpdateOptions().SetTieredStorage(true).TieredStorage().has_value(); }},
              {"local_retention_age",
               [] {
                   return TopicUpdateOptions()
                       .SetLocalRetentionAge(iggy::Expiry::NeverExpire())
                       .LocalRetentionAge()
                       .has_value();
               }},
              {"local_retention_size",
               [] {
                   return TopicUpdateOptions()
                       .SetLocalRetentionSize(iggy::MaxTopicSize::Unlimited())
                       .LocalRetentionSize()
                       .has_value();
               }},
              {"fresh_upload_interval", GeoDurationMember<TopicUpdateOptions>("fresh_upload_interval")},
              {"consumer_offset_upload_interval",
               GeoDurationMember<TopicUpdateOptions>("consumer_offset_upload_interval")},
              {"positions_publish_interval", GeoDurationMember<TopicUpdateOptions>("positions_publish_interval")},
              {"tombstone_grace", GeoDurationMember<TopicUpdateOptions>("tombstone_grace")},
              {"positions_resume_timeout", GeoDurationMember<TopicUpdateOptions>("positions_resume_timeout")},
              {"long_term_rewrite_max_interval",
               GeoDurationMember<TopicUpdateOptions>("long_term_rewrite_max_interval")},
              {"read_cache_size", ReadCacheMember<TopicUpdateOptions>("read_cache_size")},
              {"read_ahead", ReadCacheMember<TopicUpdateOptions>("read_ahead")},
          },
          {
              {"message_expiry", ResetMember<TopicUpdateOptions>(iggy::TopicOptionKey::MessageExpiry)},
              {"max_topic_size", ResetMember<TopicUpdateOptions>(iggy::TopicOptionKey::MaxTopicSize)},
              {"local_retention_age", ResetMember<TopicUpdateOptions>(iggy::TopicOptionKey::LocalRetentionAge)},
              {"local_retention_size", ResetMember<TopicUpdateOptions>(iggy::TopicOptionKey::LocalRetentionSize)},
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
              {"shadow_origin",
               [] {
                   static_cast<void>(&iggy::StreamDetails::ShadowOrigin);
                   static_cast<void>(&iggy::Stream::ShadowOrigin);
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

namespace iggy {

// Topic and TopicDetails name this class as a friend, because only a read against a server builds one.
class TopicTestPeer {
  public:
    static Topic FromBridge(ffi::Topic topic) { return Topic::FromFfi(std::move(topic)); }
    static TopicDetails FromBridge(ffi::TopicDetails topic) { return TopicDetails::FromFfi(std::move(topic)); }
};

}  // namespace iggy

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
    // An enable-only key has a setter on an update too: the update turns it on.
    const auto updatable = [](const nlohmann::json &row) {
        return row.at("mutability") == "updatable" || row.at("mutability") == "enable_only";
    };
    for (const auto &[scope, surface] : TypedSurface()) {
        SCOPED_TRACE(scope);
        EXPECT_EQ(SurfaceKeys(surface.create), CatalogKeys(scope, [&updatable](const nlohmann::json &row) {
                      return row.at("storage") == "map" && (row.at("mutability") == "create_only" || updatable(row));
                  }));
        EXPECT_EQ(SurfaceKeys(surface.update), CatalogKeys(scope, updatable));
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

// C++ cannot encode a request without sending it, so the Rust tests of the bridge check the wire bytes and HTTP
// bodies of these vectors, from the bridge fields that IggyBlockingClient fills from these typed options.
TEST(ResourceOptionsFixtureTest, GivenGeoDurationVectorsWhenSettingTypedOptionsShouldKeepEachValueAndZero) {
    ExpectTypedGeoDurations<iggy::TopicCreateOptions>(
        FixtureCase("create_topic", "geo_topic_with_durations").at("input"));
    ExpectTypedGeoDurations<iggy::TopicUpdateOptions>(
        FixtureCase("update_resource", "topic_set_geo_durations").at("input"));
}

TEST(ResourceOptionsFixtureTest, GivenTopicResponseProjectionsWhenReadingShouldExposeEachGeoDuration) {
    for (const auto &vector : Fixture().at("topic_responses")) {
        SCOPED_TRACE(vector.at("case").get<std::string>());
        const auto &expected = vector.at("expected");
        iggy::ffi::Topic summary{};
        summary.geo_durations = FfiGeoDurations(expected);
        iggy::ffi::TopicDetails details{};
        details.geo_durations = FfiGeoDurations(expected);

        const auto topic         = iggy::TopicTestPeer::FromBridge(std::move(summary));
        const auto topic_details = iggy::TopicTestPeer::FromBridge(std::move(details));

        for (const auto &[key, getter] : GeoDurationGetters<iggy::Topic>()) {
            EXPECT_EQ((topic.*getter)(), FixtureMicros(expected, key)) << key;
        }
        for (const auto &[key, getter] : GeoDurationGetters<iggy::TopicDetails>()) {
            EXPECT_EQ((topic_details.*getter)(), FixtureMicros(expected, key)) << key;
        }
    }
}

TEST(ResourceOptionsFixtureTest, GivenReadCacheVectorsWhenSettingTypedOptionsShouldKeepEachValueAndZero) {
    ExpectTypedReadCache<iggy::TopicCreateOptions>(FixtureCase("create_topic", "topic_with_read_cache").at("input"));
    ExpectTypedReadCache<iggy::TopicUpdateOptions>(FixtureCase("update_resource", "topic_set_read_cache").at("input"));
}

TEST(ResourceOptionsFixtureTest, GivenTopicResponseProjectionsWhenReadingShouldExposeTheReadCache) {
    for (const auto &vector : Fixture().at("topic_responses")) {
        SCOPED_TRACE(vector.at("case").get<std::string>());
        const auto &expected = vector.at("expected");
        iggy::ffi::Topic summary{};
        summary.read_cache = FfiReadCache(expected);
        iggy::ffi::TopicDetails details{};
        details.read_cache = FfiReadCache(expected);

        const auto topic         = iggy::TopicTestPeer::FromBridge(std::move(summary));
        const auto topic_details = iggy::TopicTestPeer::FromBridge(std::move(details));

        for (const auto &[key, getter] : ReadCacheGetters<iggy::Topic>()) {
            EXPECT_EQ((topic.*getter)(), FixtureBytes(expected, key)) << key;
        }
        for (const auto &[key, getter] : ReadCacheGetters<iggy::TopicDetails>()) {
            EXPECT_EQ((topic_details.*getter)(), FixtureBytes(expected, key)) << key;
        }
    }
}

TEST(ResourceOptionsFixtureTest, GivenDurationPastTheMicrosecondsRangeWhenReadingShouldReadTheLongest) {
    const auto longest = static_cast<std::uint64_t>(std::chrono::microseconds::max().count());
    iggy::ffi::TopicDetails details{};
    details.geo_durations.has_tombstone_grace             = true;
    details.geo_durations.tombstone_grace_micros          = std::numeric_limits<std::uint64_t>::max();
    details.geo_durations.has_positions_resume_timeout    = true;
    details.geo_durations.positions_resume_timeout_micros = longest;

    const auto topic = iggy::TopicTestPeer::FromBridge(std::move(details));

    EXPECT_EQ(topic.TombstoneGrace(), std::chrono::microseconds::max());
    EXPECT_EQ(topic.PositionsResumeTimeout(), std::chrono::microseconds::max());
    EXPECT_FALSE(topic.FreshUploadInterval().has_value());
}
