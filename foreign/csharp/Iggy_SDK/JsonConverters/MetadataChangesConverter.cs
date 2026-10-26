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

using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using Apache.Iggy.Contracts;
using Apache.Iggy.Contracts.Auth;
using Apache.Iggy.Enums;

namespace Apache.Iggy.JsonConverters;

/// <summary>
///     Reads the JSON answer of <c>GET /metadata/changes</c>. Records and items are flat objects of the envelope
///     fields and the body fields, 128-bit values are 32 lowercase hex characters, and a record or item of a group
///     or kind this build does not know is kept whole as <see cref="MetadataUnknownJsonBody" />.
/// </summary>
internal sealed class MetadataChangesConverter : JsonConverter<MetadataChanges>
{
    private const int HEX_U128_LENGTH = 32;

    private static readonly JsonSerializerOptions BodyOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        Converters = { new ResourceOptionsConverter() }
    };

    public override MetadataChanges Read(ref Utf8JsonReader reader, Type typeToConvert,
        JsonSerializerOptions options)
    {
        using var document = JsonDocument.ParseValue(ref reader);
        var answer = document.RootElement;
        try
        {
            return new MetadataChanges
            {
                Status = Name<MetadataChangeStatus>(answer, "status"),
                Phase = Name<MetadataChangePhase>(answer, "phase"),
                LogId = IsNull(answer, "log_id") ? null : HexU128(answer, "log_id"),
                OldestOp = Property(answer, "oldest_op").GetUInt64(),
                RetainedSince = Property(answer, "retained_since").GetUInt64(),
                HeadOp = Property(answer, "head_op").GetUInt64(),
                Next = IsNull(answer, "next") ? null : Cursor(Property(answer, "next")),
                HasMore = Property(answer, "has_more").GetBoolean(),
                Records = Property(answer, "records").EnumerateArray().Select(ReadRecord).ToArray(),
                Items = Property(answer, "items").EnumerateArray().Select(ReadItem).ToArray(),
                CurrentPageToken = IsNull(answer, "current_page_token") ? null : PageToken(answer)
            };
        }
        catch (Exception error) when (error is InvalidOperationException or FormatException)
        {
            // JsonElement reports a value of the wrong JSON type or range with these instead of JsonException.
            throw new JsonException("The metadata changes answer does not have the documented shape.", error);
        }
    }

    /// <summary>The answer is only ever read from a response, never written.</summary>
    public override void Write(Utf8JsonWriter writer, MetadataChanges value, JsonSerializerOptions options)
    {
        throw new NotSupportedException("Metadata changes are read from a response, not written to a request.");
    }

    private static MetadataChangeRecord ReadRecord(JsonElement record)
    {
        var groupName = Text(record, "group");
        var kindName = Text(record, "kind");
        MetadataChangeKind? kind = null;
        MetadataChangeBody body;
        if (SnakeCaseNames<MetadataChangeGroup>.TryParse(groupName, out var group)
            && SnakeCaseNames<MetadataChangeKind>.TryParse(kindName, out var knownKind))
        {
            if (MetadataChangeGroups.Of(knownKind) != group)
            {
                throw new JsonException($"Metadata change kind {kindName} does not belong to group {groupName}.");
            }

            kind = knownKind;
            body = ChangeBody(knownKind, record);
        }
        else
        {
            body = new MetadataUnknownJsonBody { Group = groupName, Kind = kindName, Json = record.GetRawText() };
        }

        return new MetadataChangeRecord
        {
            Op = Property(record, "op").GetUInt64(),
            Ordinal = Property(record, "ordinal").GetUInt32(),
            Checksum = HexU128(record, "checksum"),
            Timestamp = Property(record, "timestamp").GetUInt64(),
            ActorUserId = OptionalU32(record, "actor_user_id"),
            Kind = kind,
            Body = body
        };
    }

    private static MetadataCurrentItem ReadItem(JsonElement item)
    {
        var groupName = Text(item, "group");
        if (!SnakeCaseNames<MetadataChangeGroup>.TryParse(groupName, out var group))
        {
            return new MetadataCurrentItem
            {
                Body = new MetadataUnknownJsonBody { Group = groupName, Json = item.GetRawText() }
            };
        }

        MetadataChangeBody body = group switch
        {
            MetadataChangeGroup.Streams => StreamChange(item),
            MetadataChangeGroup.Topics => TopicItemBody(item),
            MetadataChangeGroup.Partitions => PartitionsItemBody(item),
            MetadataChangeGroup.ConsumerGroups => ConsumerGroupItemBody(item),
            MetadataChangeGroup.ConsumerGroupMembers => MemberItemBody(item),
            MetadataChangeGroup.Users => UserChange(item),
            MetadataChangeGroup.PersonalAccessTokens => TokenChange(item),
            MetadataChangeGroup.Sessions => SessionBody(item),
            _ => throw new JsonException($"Metadata current item group {groupName} has no decoder.")
        };
        return new MetadataCurrentItem { Group = group, Body = body };
    }

    private static MetadataChangeBody ChangeBody(MetadataChangeKind kind, JsonElement record)
    {
        return kind switch
        {
            MetadataChangeKind.StreamCreated or MetadataChangeKind.StreamUpdated => StreamChange(record),
            MetadataChangeKind.StreamDeleted => new MetadataStreamDeleted
            {
                StreamId = Property(record, "stream_id").GetUInt32(),
                StreamCreatedOp = Property(record, "stream_created_op").GetUInt64(),
                CreatedAt = Property(record, "created_at").GetUInt64(),
                Name = Text(record, "name"),
                RequestedOp = OptionalU64(record, "requested_op")
            },
            MetadataChangeKind.TopicCreated or MetadataChangeKind.TopicUpdated => TopicChange(record),
            MetadataChangeKind.TopicDeleted => new MetadataTopicDeleted
            {
                StreamId = Property(record, "stream_id").GetUInt32(),
                StreamCreatedOp = Property(record, "stream_created_op").GetUInt64(),
                TopicId = Property(record, "topic_id").GetUInt32(),
                TopicCreatedOp = Property(record, "topic_created_op").GetUInt64(),
                CreatedAt = Property(record, "created_at").GetUInt64(),
                Name = Text(record, "name"),
                RequestedOp = OptionalU64(record, "requested_op")
            },
            MetadataChangeKind.PartitionsChanged => new MetadataPartitionsChanged
            {
                StreamId = Property(record, "stream_id").GetUInt32(),
                StreamCreatedOp = Property(record, "stream_created_op").GetUInt64(),
                TopicId = Property(record, "topic_id").GetUInt32(),
                TopicCreatedOp = Property(record, "topic_created_op").GetUInt64(),
                PartitionsCount = Property(record, "partitions_count").GetUInt32(),
                RemovedPartitionIds = Ids(record, "removed_partition_ids"),
                RequestedOp = OptionalU64(record, "requested_op")
            },
            MetadataChangeKind.ConsumerGroupCreated => new MetadataConsumerGroupCreated
            {
                StreamId = Property(record, "stream_id").GetUInt32(),
                StreamCreatedOp = Property(record, "stream_created_op").GetUInt64(),
                TopicId = Property(record, "topic_id").GetUInt32(),
                TopicCreatedOp = Property(record, "topic_created_op").GetUInt64(),
                GroupId = Property(record, "group_id").GetUInt32(),
                Name = Text(record, "name"),
                ConsumerGroupsCount = Property(record, "consumer_groups_count").GetUInt32()
            },
            MetadataChangeKind.ConsumerGroupDeleted => new MetadataConsumerGroupDeleted
            {
                StreamId = Property(record, "stream_id").GetUInt32(),
                StreamCreatedOp = Property(record, "stream_created_op").GetUInt64(),
                TopicId = Property(record, "topic_id").GetUInt32(),
                TopicCreatedOp = Property(record, "topic_created_op").GetUInt64(),
                GroupId = Property(record, "group_id").GetUInt32(),
                Name = Text(record, "name"),
                ConsumerGroupsCount = Property(record, "consumer_groups_count").GetUInt32(),
                RequestedOp = OptionalU64(record, "requested_op")
            },
            MetadataChangeKind.ConsumerGroupMemberJoined => new MetadataMemberJoined
            {
                StreamId = Property(record, "stream_id").GetUInt32(),
                StreamCreatedOp = Property(record, "stream_created_op").GetUInt64(),
                TopicId = Property(record, "topic_id").GetUInt32(),
                TopicCreatedOp = Property(record, "topic_created_op").GetUInt64(),
                GroupId = Property(record, "group_id").GetUInt32(),
                MemberId = Property(record, "member_id").GetUInt32(),
                ClientId = HexU128(record, "client_id"),
                MembersCount = Property(record, "members_count").GetUInt32(),
                PartitionIds = Ids(record, "partition_ids")
            },
            MetadataChangeKind.ConsumerGroupMemberLeft => new MetadataMemberLeft
            {
                StreamId = Property(record, "stream_id").GetUInt32(),
                StreamCreatedOp = Property(record, "stream_created_op").GetUInt64(),
                TopicId = Property(record, "topic_id").GetUInt32(),
                TopicCreatedOp = Property(record, "topic_created_op").GetUInt64(),
                GroupId = Property(record, "group_id").GetUInt32(),
                MemberId = Property(record, "member_id").GetUInt32(),
                ClientId = HexU128(record, "client_id"),
                MembersCount = Property(record, "members_count").GetUInt32(),
                Reason = Name<MetadataMemberLeftReason>(record, "reason")
            },
            MetadataChangeKind.ConsumerGroupPartitionHandedOver => new MetadataPartitionHandedOver
            {
                StreamId = Property(record, "stream_id").GetUInt32(),
                StreamCreatedOp = Property(record, "stream_created_op").GetUInt64(),
                TopicId = Property(record, "topic_id").GetUInt32(),
                TopicCreatedOp = Property(record, "topic_created_op").GetUInt64(),
                GroupId = Property(record, "group_id").GetUInt32(),
                PartitionId = Property(record, "partition_id").GetUInt32(),
                MemberId = OptionalU32(record, "member_id")
            },
            MetadataChangeKind.UserCreated or MetadataChangeKind.UserUpdated => UserChange(record),
            MetadataChangeKind.UserDeleted or MetadataChangeKind.UserPasswordChanged => new MetadataUserIdentity
            {
                UserId = Property(record, "user_id").GetUInt32(),
                UserCreatedAt = Property(record, "user_created_at").GetUInt64(),
                Username = Text(record, "username")
            },
            MetadataChangeKind.UserPermissionsUpdated => new MetadataUserPermissionsUpdated
            {
                UserId = Property(record, "user_id").GetUInt32(),
                UserCreatedAt = Property(record, "user_created_at").GetUInt64(),
                Username = Text(record, "username"),
                Permissions = ReadPermissions(record),
                Reason = Name<MetadataPermissionsUpdatedReason>(record, "reason")
            },
            MetadataChangeKind.PersonalAccessTokenCreated => TokenChange(record),
            MetadataChangeKind.PersonalAccessTokenDeleted => new MetadataTokenDeleted
            {
                UserId = Property(record, "user_id").GetUInt32(),
                UserCreatedAt = Property(record, "user_created_at").GetUInt64(),
                Name = Text(record, "name"),
                Reason = Name<MetadataTokenDeletedReason>(record, "reason")
            },
            MetadataChangeKind.SessionRegistered => SessionBody(record),
            MetadataChangeKind.SessionClosed => new MetadataSessionClosed
            {
                UserId = Property(record, "user_id").GetUInt32(),
                UserCreatedAt = Property(record, "user_created_at").GetUInt64(),
                ClientId = HexU128(record, "client_id"),
                Session = Property(record, "session").GetUInt64(),
                Reason = Name<MetadataSessionClosedReason>(record, "reason")
            },
            _ => throw new JsonException($"Metadata change kind {kind} has no decoder.")
        };
    }

    private static MetadataStreamChange StreamChange(JsonElement body)
    {
        var unreadable = ReadOptions(body, out var options);
        return new MetadataStreamChange
        {
            StreamId = Property(body, "stream_id").GetUInt32(),
            StreamCreatedOp = Property(body, "stream_created_op").GetUInt64(),
            CreatedAt = Property(body, "created_at").GetUInt64(),
            Name = Text(body, "name"),
            Options = options?.Explicit,
            DerivedOptions = options?.Derived,
            OptionsUnreadable = unreadable
        };
    }

    private static MetadataTopicChange TopicChange(JsonElement body)
    {
        var unreadable = ReadOptions(body, out var options);
        return new MetadataTopicChange
        {
            StreamId = Property(body, "stream_id").GetUInt32(),
            StreamCreatedOp = Property(body, "stream_created_op").GetUInt64(),
            TopicId = Property(body, "topic_id").GetUInt32(),
            TopicCreatedOp = Property(body, "topic_created_op").GetUInt64(),
            CreatedAt = Property(body, "created_at").GetUInt64(),
            Name = Text(body, "name"),
            PartitionsCount = Property(body, "partitions_count").GetUInt32(),
            Options = options?.Explicit,
            DerivedOptions = options?.Derived,
            OptionsUnreadable = unreadable
        };
    }

    private static MetadataTopicItem TopicItemBody(JsonElement body)
    {
        var unreadable = ReadOptions(body, out var options);
        return new MetadataTopicItem
        {
            StreamId = Property(body, "stream_id").GetUInt32(),
            StreamCreatedOp = Property(body, "stream_created_op").GetUInt64(),
            TopicId = Property(body, "topic_id").GetUInt32(),
            TopicCreatedOp = Property(body, "topic_created_op").GetUInt64(),
            CreatedAt = Property(body, "created_at").GetUInt64(),
            Name = Text(body, "name"),
            Options = options?.Explicit,
            DerivedOptions = options?.Derived,
            OptionsUnreadable = unreadable
        };
    }

    private static MetadataPartitionsItem PartitionsItemBody(JsonElement body)
    {
        return new MetadataPartitionsItem
        {
            StreamId = Property(body, "stream_id").GetUInt32(),
            StreamCreatedOp = Property(body, "stream_created_op").GetUInt64(),
            TopicId = Property(body, "topic_id").GetUInt32(),
            TopicCreatedOp = Property(body, "topic_created_op").GetUInt64(),
            PartitionsCount = Property(body, "partitions_count").GetUInt32()
        };
    }

    private static MetadataConsumerGroupItem ConsumerGroupItemBody(JsonElement body)
    {
        return new MetadataConsumerGroupItem
        {
            StreamId = Property(body, "stream_id").GetUInt32(),
            StreamCreatedOp = Property(body, "stream_created_op").GetUInt64(),
            TopicId = Property(body, "topic_id").GetUInt32(),
            TopicCreatedOp = Property(body, "topic_created_op").GetUInt64(),
            GroupId = Property(body, "group_id").GetUInt32(),
            Name = Text(body, "name")
        };
    }

    private static MetadataMemberItem MemberItemBody(JsonElement body)
    {
        return new MetadataMemberItem
        {
            StreamId = Property(body, "stream_id").GetUInt32(),
            StreamCreatedOp = Property(body, "stream_created_op").GetUInt64(),
            TopicId = Property(body, "topic_id").GetUInt32(),
            TopicCreatedOp = Property(body, "topic_created_op").GetUInt64(),
            GroupId = Property(body, "group_id").GetUInt32(),
            MemberId = Property(body, "member_id").GetUInt32(),
            ClientId = HexU128(body, "client_id"),
            PartitionIds = Ids(body, "partition_ids")
        };
    }

    private static MetadataUserChange UserChange(JsonElement body)
    {
        var unreadable = ReadOptions(body, out var options);
        return new MetadataUserChange
        {
            UserId = Property(body, "user_id").GetUInt32(),
            UserCreatedAt = Property(body, "user_created_at").GetUInt64(),
            Username = Text(body, "username"),
            Status = Text(body, "status") switch
            {
                "active" => UserStatus.Active,
                "inactive" => UserStatus.Inactive,
                var status => throw new JsonException($"User status {status} is unknown.")
            },
            Permissions = ReadPermissions(body),
            Options = options?.Explicit,
            DerivedOptions = options?.Derived,
            OptionsUnreadable = unreadable
        };
    }

    private static MetadataTokenChange TokenChange(JsonElement body)
    {
        return new MetadataTokenChange
        {
            UserId = Property(body, "user_id").GetUInt32(),
            UserCreatedAt = Property(body, "user_created_at").GetUInt64(),
            Name = Text(body, "name"),
            Expiry = OptionalU64(body, "expiry")
        };
    }

    private static MetadataSession SessionBody(JsonElement body)
    {
        return new MetadataSession
        {
            UserId = Property(body, "user_id").GetUInt32(),
            UserCreatedAt = Property(body, "user_created_at").GetUInt64(),
            ClientId = HexU128(body, "client_id"),
            Session = Property(body, "session").GetUInt64()
        };
    }

    /// <summary>
    ///     Reads <c>options</c> and <c>options_unreadable</c>: an object with the flag absent or false, or null with
    ///     the flag true. Returns whether the state was unreadable.
    /// </summary>
    private static bool ReadOptions(JsonElement body, out ResourceOptions? options)
    {
        options = null;
        var unreadable = body.TryGetProperty("options_unreadable", out var flag) && flag.GetBoolean();
        var hasOptions = body.TryGetProperty("options", out var value) && value.ValueKind != JsonValueKind.Null;
        if (hasOptions == unreadable)
        {
            throw new JsonException("Options must be an object, or null with options_unreadable true.");
        }

        if (hasOptions)
        {
            options = value.Deserialize<ResourceOptions>(BodyOptions);
        }

        return unreadable;
    }

    private static Permissions? ReadPermissions(JsonElement body)
    {
        return Property(body, "permissions") is { ValueKind: not JsonValueKind.Null } permissions
            ? permissions.Deserialize<Permissions>(BodyOptions)
            : null;
    }

    private static uint[] Ids(JsonElement body, string name)
    {
        var ids = Property(body, name);
        var result = new uint[ids.GetArrayLength()];
        var index = 0;
        foreach (var id in ids.EnumerateArray())
        {
            result[index++] = id.GetUInt32();
        }

        return result;
    }

    private static T Name<T>(JsonElement body, string name) where T : struct, Enum
    {
        var text = Text(body, name);
        return SnakeCaseNames<T>.TryParse(text, out var value)
            ? value
            : throw new JsonException($"Metadata changes {name} {text} is unknown.");
    }

    private static MetadataChangeCursor Cursor(JsonElement element)
    {
        return MetadataChangeCursor.TryParse(element.GetString(), out var cursor)
            ? cursor
            : throw new JsonException("Metadata changes next is not a canonical change cursor.");
    }

    private static MetadataCurrentPageToken PageToken(JsonElement answer)
    {
        return MetadataCurrentPageToken.TryParse(Property(answer, "current_page_token").GetString(), out var token)
            ? token
            : throw new JsonException("Metadata changes current_page_token is not a canonical page token.");
    }

    private static UInt128 HexU128(JsonElement body, string name)
    {
        var text = Text(body, name);
        var canonical = text.Length == HEX_U128_LENGTH && text.All(character => char.IsAsciiDigit(character)
            || character is >= 'a' and <= 'f');
        return canonical
            ? UInt128.Parse(text, NumberStyles.AllowHexSpecifier, CultureInfo.InvariantCulture)
            : throw new JsonException($"Metadata changes {name} must be {HEX_U128_LENGTH} lowercase hex characters.");
    }

    private static ulong? OptionalU64(JsonElement body, string name)
    {
        return body.TryGetProperty(name, out var value) && value.ValueKind != JsonValueKind.Null
            ? value.GetUInt64()
            : null;
    }

    private static uint? OptionalU32(JsonElement body, string name)
    {
        return body.TryGetProperty(name, out var value) && value.ValueKind != JsonValueKind.Null
            ? value.GetUInt32()
            : null;
    }

    private static string Text(JsonElement body, string name)
    {
        return Property(body, name).GetString() ?? throw new JsonException($"Metadata changes {name} is null.");
    }

    private static bool IsNull(JsonElement body, string name)
    {
        return Property(body, name).ValueKind == JsonValueKind.Null;
    }

    private static JsonElement Property(JsonElement body, string name)
    {
        return body.TryGetProperty(name, out var value)
            ? value
            : throw new JsonException($"Metadata changes {name} is missing.");
    }
}
