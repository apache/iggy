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

// The canonical form of a metadata changes answer is the HTTP JSON that the fixture
// core/common/fixtures/metadata_changes.json names "expected". The views below follow the fixture test of the SDK,
// Iggy_SDK_Tests/MapperTests/MetadataChangesTests.cs.

using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using Apache.Iggy.Contracts;
using Apache.Iggy.Contracts.Auth;
using Apache.Iggy.Headers;

namespace Apache.Iggy.ConformanceAgent;

internal static class MetadataView
{
    private static readonly JsonSerializerOptions PermissionsJson = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower
    };

    internal static JsonObject Changes(MetadataChanges changes)
    {
        return new JsonObject
        {
            ["status"] = Name(changes.Status),
            ["phase"] = Name(changes.Phase),
            ["log_id"] = changes.LogId is { } logId ? Hex(logId) : null,
            ["oldest_op"] = changes.OldestOp,
            ["retained_since"] = changes.RetainedSince,
            ["head_op"] = changes.HeadOp,
            ["next"] = changes.Next?.ToString(),
            ["has_more"] = changes.HasMore,
            ["records"] = new JsonArray([.. changes.Records.Select(Record)]),
            ["items"] = new JsonArray([.. changes.Items.Select(Item)]),
            ["current_page_token"] = changes.CurrentPageToken?.ToString()
        };
    }

    private static JsonNode Record(MetadataChangeRecord record)
    {
        var view = new JsonObject
        {
            ["op"] = record.Op,
            ["ordinal"] = record.Ordinal,
            ["checksum"] = Hex(record.Checksum),
            ["timestamp"] = record.Timestamp
        };
        if (record.ActorUserId is { } actor)
        {
            view["actor_user_id"] = actor;
        }

        if (record.Body is MetadataUnknownBody unknown)
        {
            view["group"] = unknown.Group.ToString(CultureInfo.InvariantCulture);
            view["kind"] = unknown.Kind?.ToString(CultureInfo.InvariantCulture);
            view["body"] = Convert.ToBase64String(unknown.Body);
            return view;
        }

        view["group"] = Name(record.Group ?? throw new FormatException("A known record without a group."));
        view["kind"] = Name(record.Kind ?? throw new FormatException("A known record without a kind."));
        AddBody(view, record.Body);
        return view;
    }

    private static JsonNode Item(MetadataCurrentItem item)
    {
        if (item.Body is MetadataUnknownBody unknown)
        {
            return new JsonObject
            {
                ["group"] = unknown.Group.ToString(CultureInfo.InvariantCulture),
                ["body"] = Convert.ToBase64String(unknown.Body)
            };
        }

        var view = new JsonObject
        {
            ["group"] = Name(item.Group ?? throw new FormatException("A known item without a group."))
        };
        AddBody(view, item.Body);
        return view;
    }

    private static void AddBody(JsonObject view, MetadataChangeBody body)
    {
        switch (body)
        {
            case MetadataStreamChange stream:
                AddStreamKey(view, stream.StreamId, stream.StreamCreatedOp);
                view["created_at"] = stream.CreatedAt;
                view["name"] = stream.Name;
                AddOptions(view, stream.Options, stream.DerivedOptions, stream.OptionsUnreadable);
                break;
            case MetadataStreamDeleted stream:
                AddStreamKey(view, stream.StreamId, stream.StreamCreatedOp);
                view["created_at"] = stream.CreatedAt;
                view["name"] = stream.Name;
                AddOptional(view, "requested_op", stream.RequestedOp);
                break;
            case MetadataTopicChange topic:
                AddTopicKey(view, topic.StreamId, topic.StreamCreatedOp, topic.TopicId, topic.TopicCreatedOp);
                view["created_at"] = topic.CreatedAt;
                view["name"] = topic.Name;
                view["partitions_count"] = topic.PartitionsCount;
                AddOptions(view, topic.Options, topic.DerivedOptions, topic.OptionsUnreadable);
                break;
            case MetadataTopicItem topic:
                AddTopicKey(view, topic.StreamId, topic.StreamCreatedOp, topic.TopicId, topic.TopicCreatedOp);
                view["created_at"] = topic.CreatedAt;
                view["name"] = topic.Name;
                AddOptions(view, topic.Options, topic.DerivedOptions, topic.OptionsUnreadable);
                break;
            case MetadataTopicDeleted topic:
                AddTopicKey(view, topic.StreamId, topic.StreamCreatedOp, topic.TopicId, topic.TopicCreatedOp);
                view["created_at"] = topic.CreatedAt;
                view["name"] = topic.Name;
                AddOptional(view, "requested_op", topic.RequestedOp);
                break;
            case MetadataPartitionsChanged partitions:
                AddTopicKey(view, partitions.StreamId, partitions.StreamCreatedOp, partitions.TopicId,
                    partitions.TopicCreatedOp);
                view["partitions_count"] = partitions.PartitionsCount;
                view["removed_partition_ids"] = Ids(partitions.RemovedPartitionIds);
                AddOptional(view, "requested_op", partitions.RequestedOp);
                break;
            case MetadataPartitionsItem partitions:
                AddTopicKey(view, partitions.StreamId, partitions.StreamCreatedOp, partitions.TopicId,
                    partitions.TopicCreatedOp);
                view["partitions_count"] = partitions.PartitionsCount;
                break;
            case MetadataConsumerGroupCreated group:
                AddTopicKey(view, group.StreamId, group.StreamCreatedOp, group.TopicId, group.TopicCreatedOp);
                view["group_id"] = group.GroupId;
                view["name"] = group.Name;
                view["consumer_groups_count"] = group.ConsumerGroupsCount;
                break;
            case MetadataConsumerGroupDeleted group:
                AddTopicKey(view, group.StreamId, group.StreamCreatedOp, group.TopicId, group.TopicCreatedOp);
                view["group_id"] = group.GroupId;
                view["name"] = group.Name;
                view["consumer_groups_count"] = group.ConsumerGroupsCount;
                AddOptional(view, "requested_op", group.RequestedOp);
                break;
            case MetadataConsumerGroupItem group:
                AddTopicKey(view, group.StreamId, group.StreamCreatedOp, group.TopicId, group.TopicCreatedOp);
                view["group_id"] = group.GroupId;
                view["name"] = group.Name;
                break;
            case MetadataMemberJoined member:
                AddTopicKey(view, member.StreamId, member.StreamCreatedOp, member.TopicId, member.TopicCreatedOp);
                view["group_id"] = member.GroupId;
                view["member_id"] = member.MemberId;
                view["client_id"] = Hex(member.ClientId);
                view["members_count"] = member.MembersCount;
                view["partition_ids"] = Ids(member.PartitionIds);
                break;
            case MetadataMemberLeft member:
                AddTopicKey(view, member.StreamId, member.StreamCreatedOp, member.TopicId, member.TopicCreatedOp);
                view["group_id"] = member.GroupId;
                view["member_id"] = member.MemberId;
                view["client_id"] = Hex(member.ClientId);
                view["members_count"] = member.MembersCount;
                view["reason"] = Name(member.Reason);
                break;
            case MetadataPartitionHandedOver handover:
                AddTopicKey(view, handover.StreamId, handover.StreamCreatedOp, handover.TopicId,
                    handover.TopicCreatedOp);
                view["group_id"] = handover.GroupId;
                view["partition_id"] = handover.PartitionId;
                AddOptional(view, "member_id", handover.MemberId);
                break;
            case MetadataMemberItem member:
                AddTopicKey(view, member.StreamId, member.StreamCreatedOp, member.TopicId, member.TopicCreatedOp);
                view["group_id"] = member.GroupId;
                view["member_id"] = member.MemberId;
                view["client_id"] = Hex(member.ClientId);
                view["partition_ids"] = Ids(member.PartitionIds);
                break;
            case MetadataUserChange user:
                AddUserKey(view, user.UserId, user.UserCreatedAt);
                view["username"] = user.Username;
                view["status"] = Canonical.StatusName(user.Status);
                view["permissions"] = Permissions(user.Permissions);
                AddOptions(view, user.Options, user.DerivedOptions, user.OptionsUnreadable);
                break;
            case MetadataUserIdentity user:
                AddUserKey(view, user.UserId, user.UserCreatedAt);
                view["username"] = user.Username;
                break;
            case MetadataUserPermissionsUpdated user:
                AddUserKey(view, user.UserId, user.UserCreatedAt);
                view["username"] = user.Username;
                view["permissions"] = Permissions(user.Permissions);
                view["reason"] = Name(user.Reason);
                break;
            case MetadataTokenChange token:
                AddUserKey(view, token.UserId, token.UserCreatedAt);
                view["name"] = token.Name;
                AddOptional(view, "expiry", token.Expiry);
                break;
            case MetadataTokenDeleted token:
                AddUserKey(view, token.UserId, token.UserCreatedAt);
                view["name"] = token.Name;
                view["reason"] = Name(token.Reason);
                break;
            case MetadataSession session:
                AddUserKey(view, session.UserId, session.UserCreatedAt);
                view["client_id"] = Hex(session.ClientId);
                view["session"] = session.Session;
                break;
            case MetadataSessionClosed session:
                AddUserKey(view, session.UserId, session.UserCreatedAt);
                view["client_id"] = Hex(session.ClientId);
                view["session"] = session.Session;
                view["reason"] = Name(session.Reason);
                break;
            default:
                throw new FormatException($"An unexpected metadata body {body.GetType().Name}.");
        }
    }

    private static void AddStreamKey(JsonObject view, uint streamId, ulong streamCreatedOp)
    {
        view["stream_id"] = streamId;
        view["stream_created_op"] = streamCreatedOp;
    }

    private static void AddTopicKey(JsonObject view, uint streamId, ulong streamCreatedOp, uint topicId,
        ulong topicCreatedOp)
    {
        AddStreamKey(view, streamId, streamCreatedOp);
        view["topic_id"] = topicId;
        view["topic_created_op"] = topicCreatedOp;
    }

    private static void AddUserKey(JsonObject view, uint userId, ulong userCreatedAt)
    {
        view["user_id"] = userId;
        view["user_created_at"] = userCreatedAt;
    }

    private static void AddOptional(JsonObject view, string name, ulong? value)
    {
        if (value is { } present)
        {
            view[name] = present;
        }
    }

    private static void AddOptional(JsonObject view, string name, uint? value)
    {
        if (value is { } present)
        {
            view[name] = present;
        }
    }

    private static void AddOptions(JsonObject view, Dictionary<HeaderKey, HeaderValue>? options,
        Dictionary<HeaderKey, HeaderValue>? derivedOptions, bool unreadable)
    {
        if (unreadable)
        {
            view["options"] = null;
            view["options_unreadable"] = true;
            return;
        }

        view["options"] = Canonical.Node(Canonical.Options(options, derivedOptions));
    }

    // The get_user JSON shape with stream permissions, as the change log renders them.
    private static JsonNode? Permissions(Permissions? permissions)
    {
        return JsonSerializer.SerializeToNode(permissions, PermissionsJson);
    }

    private static JsonArray Ids(IReadOnlyList<uint> ids)
    {
        return new JsonArray([.. ids.Select(id => (JsonNode)id)]);
    }

    private static string Name<T>(T value) where T : struct, Enum
    {
        return JsonNamingPolicy.SnakeCaseLower.ConvertName(value.ToString());
    }

    private static string Hex(UInt128 value)
    {
        return value.ToString("x32", CultureInfo.InvariantCulture);
    }
}
