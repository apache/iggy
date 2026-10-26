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

// Unique step-class prefix so the generated CukeObject<n> symbols do not clash with the other
// step files when linked together (see background_steps.cpp for details).
#define CUKE_OBJECT_PREFIX IggyBddMetadataChanges

#include <gtest/gtest.h>

#include <cucumber-cpp/autodetect.hpp>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <variant>
#include <vector>

#include "world.hpp"

namespace {

constexpr std::uint32_t kRootUserId    = 0;
constexpr std::uint32_t kUnauthorized  = 41;
constexpr int kFollowAttempts          = 100;
constexpr auto kFollowInterval         = std::chrono::milliseconds(100);
constexpr const char *kHiddenOptions[] = {"encryption_key", "encryption_key_wrapped", "critical_keys"};

// Walks the wire codes until to_string names the value, so no second name table is needed.
template <typename Code>
Code CodeNamed(const std::string &name) {
    for (unsigned value = 0; value <= 255; ++value) {
        try {
            if (iggy::to_string(static_cast<Code>(value)) == name) {
                return static_cast<Code>(value);
            }
        } catch (const std::invalid_argument &) {
        }
    }
    throw std::invalid_argument("Unknown name " + name);
}

template <typename Code>
std::vector<Code> CodesNamed(const std::string &names) {
    std::vector<Code> codes;
    std::stringstream stream(names);
    std::string name;
    while (std::getline(stream, name, ',')) {
        codes.push_back(CodeNamed<Code>(name));
    }
    return codes;
}

iggy::MetadataChanges ReadChanges(bdd::GlobalContext &context, iggy::MetadataChangesQuery query) {
    return bdd::sdk_client(context).GetMetadataChanges(
        query.SetGroups(context.last_change_groups).SetLimit(iggy::MetadataChangesQuery::kMaxLimit));
}

std::optional<std::uint32_t> ChangeStreamId(const iggy::MetadataChangeRecord &record) {
    if (const auto *stream = std::get_if<iggy::MetadataStreamChange>(&record.Change())) {
        return stream->StreamId();
    }
    if (const auto *stream = std::get_if<iggy::MetadataStreamDeleted>(&record.Change())) {
        return stream->StreamId();
    }
    if (const auto *topic = std::get_if<iggy::MetadataTopicChange>(&record.Change())) {
        return topic->StreamId();
    }
    if (const auto *topic = std::get_if<iggy::MetadataTopicDeleted>(&record.Change())) {
        return topic->StreamId();
    }
    if (const auto *partitions = std::get_if<iggy::MetadataPartitionsChanged>(&record.Change())) {
        return partitions->StreamId();
    }
    return std::nullopt;
}

const iggy::MetadataChangeRecord &Followed(const bdd::GlobalContext &context, const std::string &kind) {
    const auto wanted = CodeNamed<iggy::MetadataChangeKind>(kind);
    for (const auto &record : context.last_changes) {
        if (record.Kind() == wanted) {
            return record;
        }
    }
    throw std::runtime_error("No followed change of kind " + kind);
}

const iggy::ResourceOptions &FollowedStreamOptions(const bdd::GlobalContext &context, const std::string &kind) {
    const auto &stream = std::get<iggy::MetadataStreamChange>(Followed(context, kind).Change());
    if (!stream.Options()) {
        throw std::runtime_error("Stream options must stay readable");
    }
    return *stream.Options();
}

std::optional<std::string> OptionText(const iggy::ResourceOptions &options, const std::string &key) {
    for (const auto *entries : {&options.Explicit(), &options.Derived()}) {
        const auto entry = entries->find(key);
        if (entry != entries->end()) {
            const auto &bytes = entry->second.Value();
            return std::string(bytes.begin(), bytes.end());
        }
    }
    return std::nullopt;
}

}  // namespace

GIVEN("^I hold the latest metadata change cursor for the groups \"([^\"]+)\"$") {
    REGEX_PARAM(std::string, groups);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    context->last_change_groups = CodesNamed<iggy::MetadataChangeGroup>(groups);
    const auto page             = ReadChanges(*context, iggy::MetadataChangesQuery::Latest());
    ASSERT_EQ(page.Status(), iggy::MetadataChangeStatus::Ok);
    ASSERT_TRUE(page.Next().has_value());
    context->last_change_cursor = page.Next();
}

WHEN("^I delete the stream$") {
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    bdd::sdk_client(*context).DeleteStream(iggy::Identifier::Numeric(context->last_stream_id.value()));
}

WHEN("^I create and delete ([0-9]+) streams named \"([^\"]+)\"$") {
    REGEX_PARAM(int, count);
    REGEX_PARAM(std::string, prefix);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    auto &client = bdd::sdk_client(*context);
    for (int index = 0; index < count; ++index) {
        const auto stream = client.CreateStream(prefix + "-" + std::to_string(index));
        client.DeleteStream(iggy::Identifier::Numeric(stream.Id()));
    }
}

THEN("^following the metadata changes should return the kinds \"([^\"]+)\" for the stream$") {
    REGEX_PARAM(std::string, kinds);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    const auto expected  = CodesNamed<iggy::MetadataChangeKind>(kinds);
    const auto stream_id = context->last_stream_id.value();
    auto cursor          = context->last_change_cursor.value();
    std::vector<iggy::MetadataChangeRecord> records;
    // A two phase delete commits its record after the call returns.
    for (int attempt = 0; attempt < kFollowAttempts; ++attempt) {
        const auto page = ReadChanges(*context, iggy::MetadataChangesQuery::After(cursor));
        ASSERT_EQ(page.Status(), iggy::MetadataChangeStatus::Ok);
        cursor = page.Next().value();
        records.insert(records.end(), page.Records().begin(), page.Records().end());
        if (page.HasMore()) {
            continue;
        }
        const auto mine = std::count_if(records.begin(), records.end(), [stream_id](const auto &record) {
            return ChangeStreamId(record) == stream_id;
        });
        if (static_cast<std::size_t>(mine) >= expected.size()) {
            break;
        }
        std::this_thread::sleep_for(kFollowInterval);
    }

    context->last_changes.clear();
    std::vector<iggy::MetadataChangeKind> followed;
    for (const auto &record : records) {
        const auto group = record.Group();
        ASSERT_TRUE(group.has_value());
        EXPECT_NE(std::find(context->last_change_groups.begin(), context->last_change_groups.end(), *group),
                  context->last_change_groups.end())
            << "record of op " << record.Op() << " is outside the requested groups";
        if (ChangeStreamId(record) == stream_id) {
            followed.push_back(record.Kind().value());
            context->last_changes.push_back(record);
        }
    }
    EXPECT_EQ(followed, expected);
}

THEN("^each followed change should name the root user as its actor$") {
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    for (const auto &record : context->last_changes) {
        EXPECT_EQ(record.ActorUserId(), kRootUserId) << "record of op " << record.Op();
    }
}

THEN("^the followed \"([^\"]+)\" change should carry a requested op before its own op$") {
    REGEX_PARAM(std::string, kind);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    const auto &record  = Followed(*context, kind);
    const auto &deleted = std::get<iggy::MetadataStreamDeleted>(record.Change());
    ASSERT_TRUE(deleted.RequestedOp().has_value());
    EXPECT_LT(*deleted.RequestedOp(), record.Op());
}

THEN("^the followed \"([^\"]+)\" change should report encryption key source \"([^\"]+)\"$") {
    REGEX_PARAM(std::string, kind);
    REGEX_PARAM(std::string, source);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    EXPECT_EQ(OptionText(FollowedStreamOptions(*context, kind), "encryption_key_source"), source);
}

THEN("^the followed \"([^\"]+)\" change should carry the stream key fingerprint$") {
    REGEX_PARAM(std::string, kind);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    ASSERT_TRUE(context->last_stream_key.has_value());
    EXPECT_EQ(OptionText(FollowedStreamOptions(*context, kind), "encryption_key_fingerprint"),
              iggy::EncryptionKeyFingerprint(*context->last_stream_key));
}

THEN("^the followed changes should expose no encryption key material$") {
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    for (const auto &record : context->last_changes) {
        const auto *stream = std::get_if<iggy::MetadataStreamChange>(&record.Change());
        if (stream == nullptr) {
            continue;
        }
        ASSERT_TRUE(stream->Options().has_value()) << "stream options must stay readable";
        for (const auto *hidden : kHiddenOptions) {
            EXPECT_EQ(stream->Options()->Explicit().count(hidden), 0u) << hidden << " must never be returned";
            EXPECT_EQ(stream->Options()->Derived().count(hidden), 0u) << hidden << " must never be returned";
        }
    }
}

THEN("^following the metadata changes should answer \"([^\"]+)\"$") {
    REGEX_PARAM(std::string, status);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    const auto expected = CodeNamed<iggy::MetadataChangeStatus>(status);
    const auto page     = ReadChanges(*context, iggy::MetadataChangesQuery::After(context->last_change_cursor.value()));
    EXPECT_EQ(page.Status(), expected);
    if (expected != iggy::MetadataChangeStatus::Ok) {
        EXPECT_TRUE(page.Records().empty());
        EXPECT_FALSE(page.Next().has_value());
    }
}

WHEN("^I read the current state for the groups \"([^\"]+)\"$") {
    REGEX_PARAM(std::string, groups);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    context->last_change_groups = CodesNamed<iggy::MetadataChangeGroup>(groups);
    context->last_current_items.clear();
    auto query = iggy::MetadataChangesQuery::Current();
    while (true) {
        const auto page = ReadChanges(*context, query);
        ASSERT_EQ(page.Status(), iggy::MetadataChangeStatus::Ok);
        context->last_current_items.insert(context->last_current_items.end(), page.Items().begin(), page.Items().end());
        if (!page.CurrentPageToken()) {
            ASSERT_TRUE(page.Next().has_value());
            context->last_change_cursor = page.Next();
            break;
        }
        query = iggy::MetadataChangesQuery::Current(*page.CurrentPageToken());
    }
}

THEN("^the current state should list the same streams as the stream list$") {
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    std::map<std::uint32_t, std::string> listed;
    for (const auto &stream : bdd::sdk_client(*context).GetStreams()) {
        listed.emplace(stream.Id(), stream.Name());
    }
    std::map<std::uint32_t, std::string> projected;
    for (const auto &item : context->last_current_items) {
        if (const auto *stream = std::get_if<iggy::MetadataStreamChange>(&item.Value())) {
            projected.emplace(stream->StreamId(), stream->Name());
        }
    }
    EXPECT_EQ(projected, listed);
}

GIVEN("^a user named \"([^\"]+)\" with password \"([^\"]+)\" and no server permissions exists$") {
    REGEX_PARAM(std::string, username);
    REGEX_PARAM(std::string, password);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    const auto user = bdd::sdk_client(*context).CreateUser(
        username, password, iggy::UserStatus::Active,
        iggy::Permissions().SetGlobal(iggy::GlobalPermissions().SetReadServers(true)), iggy::UserCreateOptions());
    context->last_user_id = user.Id();
}

WHEN("^I authenticate as \"([^\"]+)\" with password \"([^\"]+)\"$") {
    REGEX_PARAM(std::string, username);
    REGEX_PARAM(std::string, password);
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    auto &client = bdd::sdk_client(*context);
    client.Logout();
    client.Login(username, password);
}

THEN("^reading the metadata changes should be refused as unauthorized$") {
    cucumber::ScenarioScope<bdd::GlobalContext> context;

    context->last_change_groups.clear();
    try {
        ReadChanges(*context, iggy::MetadataChangesQuery::Earliest());
        FAIL() << "manage_servers is required";
    } catch (const iggy::IggyException &error) {
        EXPECT_EQ(error.Code(), kUnauthorized) << error.what();
    }
}
