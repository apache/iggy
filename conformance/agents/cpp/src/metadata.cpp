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

// The bridge carries a metadata change as wire codes and one flat body with every field that a
// body can have. The views below name the codes and keep the fields of each kind, as the fixture
// core/common/fixtures/metadata_changes.json renders them.

#include <array>
#include <string>
#include <string_view>

#include "canonical.hpp"

namespace agent {

namespace {

enum class Group : std::uint8_t {
    Streams = 1,
    Topics,
    Partitions,
    ConsumerGroups,
    ConsumerGroupMembers,
    Users,
    PersonalAccessTokens,
    Sessions,
};

enum class Kind : std::uint8_t {
    StreamCreated                    = 1,
    StreamUpdated                    = 2,
    StreamDeleted                    = 3,
    TopicCreated                     = 10,
    TopicUpdated                     = 11,
    TopicDeleted                     = 12,
    PartitionsChanged                = 20,
    ConsumerGroupCreated             = 30,
    ConsumerGroupDeleted             = 31,
    ConsumerGroupMemberJoined        = 40,
    ConsumerGroupMemberLeft          = 41,
    ConsumerGroupPartitionHandedOver = 42,
    UserCreated                      = 50,
    UserUpdated                      = 51,
    UserDeleted                      = 52,
    UserPasswordChanged              = 53,
    UserPermissionsUpdated           = 54,
    PersonalAccessTokenCreated       = 60,
    PersonalAccessTokenDeleted       = 61,
    SessionRegistered                = 70,
    SessionClosed                    = 71,
};

// Names indexed by their wire code.
constexpr std::array<std::string_view, 4> kStatusNames               = {"ok", "behind", "expired", "reset"};
constexpr std::array<std::string_view, 2> kPhaseNames                = {"changes", "current"};
constexpr std::array<std::string_view, 9> kGroupNames                = {"",
                                                                        "streams",
                                                                        "topics",
                                                                        "partitions",
                                                                        "consumer_groups",
                                                                        "consumer_group_members",
                                                                        "users",
                                                                        "personal_access_tokens",
                                                                        "sessions"};
constexpr std::array<std::string_view, 2> kMemberLeftReasons         = {"left", "session_ended"};
constexpr std::array<std::string_view, 2> kTokenDeletedReasons       = {"deleted", "expired"};
constexpr std::array<std::string_view, 3> kSessionClosedReasons      = {"logout", "expired", "revoked"};
constexpr std::array<std::string_view, 2> kPermissionsUpdatedReasons = {"updated", "resource_deleted"};

template <std::size_t Size>
std::string Name(const std::array<std::string_view, Size> &names, std::uint8_t code, const char *what) {
    if (code >= Size || names[code].empty()) {
        throw AgentFailure("result: an unknown " + std::string(what) + " " + std::to_string(code));
    }
    return std::string(names[code]);
}

std::string ChangeKindName(std::uint8_t code) {
    switch (static_cast<Kind>(code)) {
        case Kind::StreamCreated:
            return "stream_created";
        case Kind::StreamUpdated:
            return "stream_updated";
        case Kind::StreamDeleted:
            return "stream_deleted";
        case Kind::TopicCreated:
            return "topic_created";
        case Kind::TopicUpdated:
            return "topic_updated";
        case Kind::TopicDeleted:
            return "topic_deleted";
        case Kind::PartitionsChanged:
            return "partitions_changed";
        case Kind::ConsumerGroupCreated:
            return "consumer_group_created";
        case Kind::ConsumerGroupDeleted:
            return "consumer_group_deleted";
        case Kind::ConsumerGroupMemberJoined:
            return "consumer_group_member_joined";
        case Kind::ConsumerGroupMemberLeft:
            return "consumer_group_member_left";
        case Kind::ConsumerGroupPartitionHandedOver:
            return "consumer_group_partition_handed_over";
        case Kind::UserCreated:
            return "user_created";
        case Kind::UserUpdated:
            return "user_updated";
        case Kind::UserDeleted:
            return "user_deleted";
        case Kind::UserPasswordChanged:
            return "user_password_changed";
        case Kind::UserPermissionsUpdated:
            return "user_permissions_updated";
        case Kind::PersonalAccessTokenCreated:
            return "personal_access_token_created";
        case Kind::PersonalAccessTokenDeleted:
            return "personal_access_token_deleted";
        case Kind::SessionRegistered:
            return "session_registered";
        case Kind::SessionClosed:
            return "session_closed";
    }
    throw AgentFailure("result: an unknown metadata change kind " + std::to_string(code));
}

void AddStreamKey(Json &view, const ffi::MetadataChangeBody &body) {
    view["stream_id"]         = body.stream_id;
    view["stream_created_op"] = body.stream_created_op;
}

void AddTopicKey(Json &view, const ffi::MetadataChangeBody &body) {
    AddStreamKey(view, body);
    view["topic_id"]         = body.topic_id;
    view["topic_created_op"] = body.topic_created_op;
}

void AddUserKey(Json &view, const ffi::MetadataChangeBody &body) {
    view["user_id"]         = body.user_id;
    view["user_created_at"] = body.user_created_at;
}

void AddName(Json &view, const ffi::MetadataChangeBody &body, const char *field = "name") {
    view[field] = std::string(body.name);
}

void AddRequestedOp(Json &view, const ffi::MetadataChangeBody &body) {
    if (body.has_requested_op) {
        view["requested_op"] = body.requested_op;
    }
}

// Options that the server holds in a form this build cannot read render as null with a marker.
void AddOptions(Json &view, const ffi::MetadataChangeBody &body) {
    if (body.options_readable) {
        view["options"] = OptionsJson(body.options, body.derived_options);
    } else {
        view["options"]            = nullptr;
        view["options_unreadable"] = true;
    }
}

void AddClient(Json &view, const ffi::MetadataChangeBody &body) {
    view["client_id"] = Hex128(body.client_id_hi, body.client_id_lo);
}

Json PartitionIds(const ffi::MetadataChangeBody &body) {
    return std::vector<std::uint32_t>(body.partition_ids.begin(), body.partition_ids.end());
}

// The permissions in the get_user JSON shape, with stream permissions. The bridge gives an absent
// and an empty map of stream or topic permissions alike, as an empty list.
Json PermissionsJson(const ffi::MetadataChangeBody &body) {
    if (!body.has_permissions) {
        return nullptr;
    }
    Json streams;
    for (const auto &stream : body.permissions.streams) {
        const auto &granted = stream.permissions;
        Json topics;
        for (const auto &topic : granted.topics) {
            topics[std::to_string(topic.topic_id)] = {
                {"manage_topic", topic.permissions.manage_topic},
                {"read_topic", topic.permissions.read_topic},
                {"poll_messages", topic.permissions.poll_messages},
                {"send_messages", topic.permissions.send_messages},
            };
        }
        streams[std::to_string(stream.stream_id)] = {
            {"manage_stream", granted.manage_stream},
            {"read_stream", granted.read_stream},
            {"manage_topics", granted.manage_topics},
            {"read_topics", granted.read_topics},
            {"poll_messages", granted.poll_messages},
            {"send_messages", granted.send_messages},
            {"topics", std::move(topics)},
        };
    }
    return {{"global", GlobalPermissionsJson(body.permissions.global)}, {"streams", std::move(streams)}};
}

void AddStream(Json &view, const ffi::MetadataChangeBody &body) {
    AddStreamKey(view, body);
    view["created_at"] = body.created_at;
    AddName(view, body);
    AddOptions(view, body);
}

void AddUser(Json &view, const ffi::MetadataChangeBody &body) {
    AddUserKey(view, body);
    AddName(view, body, "username");
    view["status"]      = StatusName(body.status);
    view["permissions"] = PermissionsJson(body);
    AddOptions(view, body);
}

void AddToken(Json &view, const ffi::MetadataChangeBody &body) {
    AddUserKey(view, body);
    AddName(view, body);
    if (body.has_expiry) {
        view["expiry"] = body.expiry;
    }
}

void AddSession(Json &view, const ffi::MetadataChangeBody &body) {
    AddUserKey(view, body);
    AddClient(view, body);
    view["session"] = body.session;
}

void AddChange(Json &view, std::uint8_t kind, const ffi::MetadataChangeBody &body) {
    switch (static_cast<Kind>(kind)) {
        case Kind::StreamCreated:
        case Kind::StreamUpdated:
            AddStream(view, body);
            return;
        case Kind::StreamDeleted:
            AddStreamKey(view, body);
            view["created_at"] = body.created_at;
            AddName(view, body);
            AddRequestedOp(view, body);
            return;
        case Kind::TopicCreated:
        case Kind::TopicUpdated:
            AddTopicKey(view, body);
            view["created_at"] = body.created_at;
            AddName(view, body);
            view["partitions_count"] = body.partitions_count;
            AddOptions(view, body);
            return;
        case Kind::TopicDeleted:
            AddTopicKey(view, body);
            view["created_at"] = body.created_at;
            AddName(view, body);
            AddRequestedOp(view, body);
            return;
        case Kind::PartitionsChanged:
            AddTopicKey(view, body);
            view["partitions_count"]      = body.partitions_count;
            view["removed_partition_ids"] = PartitionIds(body);
            AddRequestedOp(view, body);
            return;
        case Kind::ConsumerGroupCreated:
        case Kind::ConsumerGroupDeleted:
            AddTopicKey(view, body);
            view["group_id"] = body.group_id;
            AddName(view, body);
            view["consumer_groups_count"] = body.consumer_groups_count;
            AddRequestedOp(view, body);
            return;
        case Kind::ConsumerGroupMemberJoined:
            AddTopicKey(view, body);
            view["group_id"]  = body.group_id;
            view["member_id"] = body.member_id;
            AddClient(view, body);
            view["members_count"] = body.members_count;
            view["partition_ids"] = PartitionIds(body);
            return;
        case Kind::ConsumerGroupMemberLeft:
            AddTopicKey(view, body);
            view["group_id"]  = body.group_id;
            view["member_id"] = body.member_id;
            AddClient(view, body);
            view["members_count"] = body.members_count;
            view["reason"]        = Name(kMemberLeftReasons, body.reason, "member left reason");
            return;
        case Kind::ConsumerGroupPartitionHandedOver:
            AddTopicKey(view, body);
            view["group_id"]     = body.group_id;
            view["partition_id"] = body.partition_id;
            if (body.has_member_id) {
                view["member_id"] = body.member_id;
            }
            return;
        case Kind::UserCreated:
        case Kind::UserUpdated:
            AddUser(view, body);
            return;
        case Kind::UserDeleted:
        case Kind::UserPasswordChanged:
            AddUserKey(view, body);
            AddName(view, body, "username");
            return;
        case Kind::UserPermissionsUpdated:
            AddUserKey(view, body);
            AddName(view, body, "username");
            view["permissions"] = PermissionsJson(body);
            view["reason"]      = Name(kPermissionsUpdatedReasons, body.reason, "permissions updated reason");
            return;
        case Kind::PersonalAccessTokenCreated:
            AddToken(view, body);
            return;
        case Kind::PersonalAccessTokenDeleted:
            AddUserKey(view, body);
            AddName(view, body);
            view["reason"] = Name(kTokenDeletedReasons, body.reason, "token deleted reason");
            return;
        case Kind::SessionRegistered:
            AddSession(view, body);
            return;
        case Kind::SessionClosed:
            AddSession(view, body);
            view["reason"] = Name(kSessionClosedReasons, body.reason, "session closed reason");
            return;
    }
    throw AgentFailure("result: an unknown metadata change kind " + std::to_string(kind));
}

void AddItem(Json &view, std::uint8_t group, const ffi::MetadataChangeBody &body) {
    switch (static_cast<Group>(group)) {
        case Group::Streams:
            AddStream(view, body);
            return;
        case Group::Topics:
            AddTopicKey(view, body);
            view["created_at"] = body.created_at;
            AddName(view, body);
            AddOptions(view, body);
            return;
        case Group::Partitions:
            AddTopicKey(view, body);
            view["partitions_count"] = body.partitions_count;
            return;
        case Group::ConsumerGroups:
            AddTopicKey(view, body);
            view["group_id"] = body.group_id;
            AddName(view, body);
            return;
        case Group::ConsumerGroupMembers:
            AddTopicKey(view, body);
            view["group_id"]  = body.group_id;
            view["member_id"] = body.member_id;
            AddClient(view, body);
            view["partition_ids"] = PartitionIds(body);
            return;
        case Group::Users:
            AddUser(view, body);
            return;
        case Group::PersonalAccessTokens:
            AddToken(view, body);
            return;
        case Group::Sessions:
            AddSession(view, body);
            return;
    }
    throw AgentFailure("result: an unknown metadata change group " + std::to_string(group));
}

// A JSON record or item of a kind this build does not know, kept whole.
Json Verbatim(const ffi::MetadataChangeBody &body) {
    return Json::parse(std::string(body.unknown_body.begin(), body.unknown_body.end()));
}

Json RecordJson(const ffi::MetadataChangeRecord &record) {
    if (record.form == ffi::MetadataChangeForm::UnknownJson) {
        return Verbatim(record.body);
    }
    Json view = {
        {"op", record.op},
        {"ordinal", record.ordinal},
        {"checksum", Hex128(record.checksum_hi, record.checksum_lo)},
        {"timestamp", record.timestamp},
    };
    if (record.has_actor_user_id) {
        view["actor_user_id"] = record.actor_user_id;
    }
    // A binary record of a kind this build does not know keeps its codes as decimal text and its
    // body in base64.
    if (record.form == ffi::MetadataChangeForm::Unknown) {
        view["group"] = std::to_string(record.group);
        view["kind"]  = std::to_string(record.kind);
        view["body"]  = Base64(record.body.unknown_body);
        return view;
    }
    view["group"] = Name(kGroupNames, record.group, "metadata change group");
    view["kind"]  = ChangeKindName(record.kind);
    AddChange(view, record.kind, record.body);
    return view;
}

Json ItemJson(const ffi::MetadataCurrentItem &item) {
    if (item.form == ffi::MetadataChangeForm::UnknownJson) {
        return Verbatim(item.body);
    }
    if (item.form == ffi::MetadataChangeForm::Unknown) {
        return {{"group", std::to_string(item.group)}, {"body", Base64(item.body.unknown_body)}};
    }
    Json view = {{"group", Name(kGroupNames, item.group, "metadata change group")}};
    AddItem(view, item.group, item.body);
    return view;
}

Json TextOrNull(bool present, const rust::String &text) {
    return present ? Json(std::string(text)) : Json();
}

}  // namespace

Json MetadataChangesJson(const ffi::MetadataChanges &changes) {
    auto records = Json::array();
    for (const auto &record : changes.records) {
        records.push_back(RecordJson(record));
    }
    auto items = Json::array();
    for (const auto &item : changes.items) {
        items.push_back(ItemJson(item));
    }
    return {
        {"status", Name(kStatusNames, changes.status, "metadata change status")},
        {"phase", Name(kPhaseNames, changes.phase, "metadata change phase")},
        {"log_id", changes.has_log_id ? Json(Hex128(changes.log_id_hi, changes.log_id_lo)) : Json()},
        {"oldest_op", changes.oldest_op},
        {"retained_since", changes.retained_since},
        {"head_op", changes.head_op},
        {"next", TextOrNull(changes.has_next, changes.next.text)},
        {"has_more", changes.has_more},
        {"records", std::move(records)},
        {"items", std::move(items)},
        {"current_page_token", TextOrNull(changes.has_current_page_token, changes.current_page_token.text)},
    };
}

}  // namespace agent
