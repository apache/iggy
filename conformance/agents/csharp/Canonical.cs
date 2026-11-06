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

// The canonical result forms of core/conformance/src/canonical.rs. The orchestrator decodes each result into its
// form and refuses any field that the form does not have.

using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using System.Text.Unicode;
using Apache.Iggy.Consumers;
using Apache.Iggy.Contracts;
using Apache.Iggy.Contracts.Auth;
using Apache.Iggy.Enums;
using Apache.Iggy.Headers;

namespace Apache.Iggy.ConformanceAgent;

internal static class Canonical
{
    internal static readonly JsonSerializerOptions Json = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower
    };

    internal static JsonNode? Node<T>(T value)
    {
        return JsonSerializer.SerializeToNode(value, Json);
    }

    internal static ulong Micros(DateTimeOffset time)
    {
        return (ulong)((time - DateTimeOffset.UnixEpoch).Ticks / TimeSpan.TicksPerMicrosecond);
    }

    internal static string StatusName(UserStatus status)
    {
        return status == UserStatus.Inactive ? "inactive" : "active";
    }

    /// <summary>The options that a client set and the options that the server derived, in one map.</summary>
    internal static SortedDictionary<string, OptionEntry> Options(Dictionary<HeaderKey, HeaderValue>? explicitOptions,
        Dictionary<HeaderKey, HeaderValue>? derivedOptions)
    {
        var options = new SortedDictionary<string, OptionEntry>(StringComparer.Ordinal);
        foreach (var (entries, isExplicit) in new[] { (derivedOptions, false), (explicitOptions, true) })
        {
            foreach (var (key, value) in entries ?? [])
            {
                options[KeyText(key)] = new OptionEntry(ValueText.Encode(value), ValueText.KindName(value.Kind),
                    isExplicit);
            }
        }

        return options;
    }

    internal static string KeyText(HeaderKey key)
    {
        return Encoding.UTF8.GetString(key.Value);
    }
}

internal sealed record OptionEntry(string Value, string Kind, bool Explicit);

internal sealed record IdentityResult(uint UserId);

internal sealed record StreamResult(
    uint Id,
    string Name,
    ulong CreatedAt,
    ulong SizeBytes,
    ulong MessagesCount,
    uint TopicsCount,
    IReadOnlyDictionary<string, OptionEntry> Options)
{
    internal static StreamResult From(StreamResponse stream)
    {
        return new StreamResult(stream.Id, stream.Name, Canonical.Micros(stream.CreatedAt), stream.Size,
            stream.MessagesCount, stream.TopicsCount, Canonical.Options(stream.Options, stream.DerivedOptions));
    }
}

internal sealed record StreamDetailsResult(
    uint Id,
    string Name,
    ulong CreatedAt,
    ulong SizeBytes,
    ulong MessagesCount,
    uint TopicsCount,
    IReadOnlyList<TopicResult> Topics,
    IReadOnlyDictionary<string, OptionEntry> Options)
{
    internal static StreamDetailsResult From(StreamResponse stream)
    {
        return new StreamDetailsResult(stream.Id, stream.Name, Canonical.Micros(stream.CreatedAt), stream.Size,
            stream.MessagesCount, stream.TopicsCount, [.. stream.Topics.Select(TopicResult.From)],
            Canonical.Options(stream.Options, stream.DerivedOptions));
    }
}

internal sealed record TopicResult(
    uint Id,
    string Name,
    ulong CreatedAt,
    ulong SizeBytes,
    ulong MessagesCount,
    uint PartitionsCount,
    IReadOnlyDictionary<string, OptionEntry> Options)
{
    internal static TopicResult From(TopicResponse topic)
    {
        return new TopicResult(topic.Id, topic.Name, Canonical.Micros(topic.CreatedAt), topic.Size,
            topic.MessagesCount, topic.PartitionsCount, Canonical.Options(topic.Options, topic.DerivedOptions));
    }
}

internal sealed record TopicDetailsResult(
    uint Id,
    string Name,
    ulong CreatedAt,
    ulong SizeBytes,
    ulong MessagesCount,
    uint PartitionsCount,
    IReadOnlyList<PartitionResult> Partitions,
    IReadOnlyDictionary<string, OptionEntry> Options)
{
    internal static TopicDetailsResult From(TopicResponse topic)
    {
        return new TopicDetailsResult(topic.Id, topic.Name, Canonical.Micros(topic.CreatedAt), topic.Size,
            topic.MessagesCount, topic.PartitionsCount, [.. (topic.Partitions ?? []).Select(PartitionResult.From)],
            Canonical.Options(topic.Options, topic.DerivedOptions));
    }
}

internal sealed record PartitionResult(
    uint Id,
    ulong CreatedAt,
    uint SegmentsCount,
    ulong CurrentOffset,
    ulong SizeBytes,
    ulong MessagesCount)
{
    internal static PartitionResult From(PartitionResponse partition)
    {
        return new PartitionResult(partition.Id, Canonical.Micros(partition.CreatedAt), partition.SegmentsCount,
            partition.CurrentOffset, partition.Size, partition.MessagesCount);
    }
}

internal sealed record PolledMessagesResult(
    uint PartitionId,
    ulong CurrentOffset,
    uint Count,
    IReadOnlyList<MessageResult> Messages)
{
    // The SDK checks the count of the reply against its messages and keeps only the messages.
    internal static PolledMessagesResult From(PolledMessages polled)
    {
        return new PolledMessagesResult(polled.PartitionId, polled.CurrentOffset, (uint)polled.Messages.Count,
            [.. polled.Messages.Select(MessageResult.From)]);
    }
}

internal sealed record MessageResult(
    ulong Offset,
    string Id,
    ulong Timestamp,
    ulong OriginTimestamp,
    ulong Checksum,
    [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    string? Payload,
    string PayloadHex,
    [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    IReadOnlyDictionary<string, HeaderEntry>? Headers)
{
    internal static MessageResult From(MessageResponse message)
    {
        var header = message.Header;
        return new MessageResult(header.Offset, header.Id.ToString(), Canonical.Micros(header.Timestamp),
            header.OriginTimestamp, header.Checksum,
            Utf8.IsValid(message.Payload) ? Encoding.UTF8.GetString(message.Payload) : null,
            Convert.ToHexStringLower(message.Payload), HeaderEntries(message.UserHeaders));
    }

    private static SortedDictionary<string, HeaderEntry>? HeaderEntries(Dictionary<HeaderKey, HeaderValue>? headers)
    {
        if (headers is not { Count: > 0 })
        {
            return null;
        }

        var entries = new SortedDictionary<string, HeaderEntry>(StringComparer.Ordinal);
        foreach (var (key, value) in headers)
        {
            entries[Canonical.KeyText(key)] = new HeaderEntry(ValueText.KindName(value.Kind), ValueText.Encode(value));
        }

        return entries;
    }
}

internal sealed record HeaderEntry(string Kind, string Value);

internal sealed record ConsumerGroupResult(
    uint Id,
    string Name,
    uint PartitionsCount,
    uint MembersCount,
    IReadOnlyList<MemberResult> Members)
{
    internal static ConsumerGroupResult From(ConsumerGroupResponse group)
    {
        return new ConsumerGroupResult(group.Id, group.Name, group.PartitionsCount, group.MembersCount,
            [.. (group.Members ?? []).Select(MemberResult.From)]);
    }
}

internal sealed record MemberResult(uint Id, uint PartitionsCount, IReadOnlyList<uint> Partitions)
{
    internal static MemberResult From(ConsumerGroupMember member)
    {
        return new MemberResult(member.Id, member.PartitionsCount, member.Partitions);
    }
}

internal sealed record RawResult(string PayloadHex);

internal sealed record OptionSpecResult(
    string Key,
    string Kind,
    bool Updatable,
    bool Resettable,
    bool Secret,
    bool ReadOnly,
    bool Critical)
{
    internal static OptionSpecResult From(OptionSpec spec)
    {
        return new OptionSpecResult(spec.Key, ValueText.KindName(spec.Kind), spec.Updatable, spec.Resettable,
            spec.Secret, spec.ReadOnly, spec.Critical);
    }
}

internal sealed record UserDetailsResult(
    uint Id,
    string Username,
    string Status,
    ulong CreatedAt,
    PermissionsResult? Permissions,
    IReadOnlyDictionary<string, OptionEntry> Options)
{
    internal static UserDetailsResult From(UserResponse user)
    {
        return new UserDetailsResult(user.Id, user.Username, Canonical.StatusName(user.Status), user.CreatedAt,
            user.Permissions is { } permissions ? PermissionsResult.From(permissions) : null,
            Canonical.Options(user.Options, user.DerivedOptions));
    }
}

/// <summary>Global permissions only. Stream permissions are not in format 1.</summary>
internal sealed record PermissionsResult(GlobalPermissionsResult Global)
{
    internal static PermissionsResult From(Permissions permissions)
    {
        return new PermissionsResult(GlobalPermissionsResult.From(permissions.Global));
    }
}

internal sealed record GlobalPermissionsResult(
    bool ManageServers,
    bool ReadServers,
    bool ManageUsers,
    bool ReadUsers,
    bool ManageStreams,
    bool ReadStreams,
    bool ManageTopics,
    bool ReadTopics,
    bool PollMessages,
    bool SendMessages)
{
    internal static GlobalPermissionsResult From(GlobalPermissions global)
    {
        return new GlobalPermissionsResult(global.ManageServers, global.ReadServers, global.ManageUsers,
            global.ReadUsers, global.ManageStreams, global.ReadStreams, global.ManageTopics, global.ReadTopics,
            global.PollMessages, global.SendMessages);
    }
}

internal sealed record ReceivedMessageResult(uint PartitionId, MessageResult Message)
{
    internal static ReceivedMessageResult From(ReceivedMessage received)
    {
        return new ReceivedMessageResult(received.PartitionId, MessageResult.From(received.Message));
    }
}

internal sealed record ConnectionInfoResult(string ServerAddress);
