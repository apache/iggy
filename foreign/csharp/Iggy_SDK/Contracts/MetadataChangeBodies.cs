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

using Apache.Iggy.Contracts.Auth;
using Apache.Iggy.Enums;
using Apache.Iggy.Headers;

namespace Apache.Iggy.Contracts;

/// <summary>
///     The body of a metadata change record or current state item. Streams are named by
///     <c>(StreamId, StreamCreatedOp)</c>, topics by their stream key plus <c>(TopicId, TopicCreatedOp)</c> and
///     users by <c>(UserId, UserCreatedAt)</c>, because ids are reused after a delete.
/// </summary>
public abstract class MetadataChangeBody
{
}

/// <summary>
///     A stream after a create or update, and a streams item. Options are the public projection the stream read
///     routes return, null with <see cref="OptionsUnreadable" /> set when the server cannot read the stored state.
/// </summary>
public sealed class MetadataStreamChange : MetadataChangeBody
{
    /// <summary>Stream id, reused after a delete.</summary>
    public required uint StreamId { get; init; }

    /// <summary>The op that created the stream.</summary>
    public required ulong StreamCreatedOp { get; init; }

    /// <summary>Creation time in microseconds since the Unix epoch.</summary>
    public required ulong CreatedAt { get; init; }

    /// <summary>Stream name.</summary>
    public required string Name { get; init; }

    /// <summary>Options a client set.</summary>
    public Dictionary<HeaderKey, HeaderValue>? Options { get; init; }

    /// <summary>Options the server filled or computes, such as the encryption key source and fingerprints.</summary>
    public Dictionary<HeaderKey, HeaderValue>? DerivedOptions { get; init; }

    /// <summary>The server holds stored critical state it cannot read. A mirror keeps its previous values.</summary>
    public bool OptionsUnreadable { get; init; }

    /// <summary>Who chose the key that encrypts the stream at rest, or null when the record reports no source.</summary>
    public StreamEncryptionSource? EncryptionKeySource =>
        OptionValues.EncryptionKeySource(Options, DerivedOptions);

    /// <summary>Fingerprint of the stream's key. Null for a plaintext stream.</summary>
    public string? EncryptionKeyFingerprint =>
        OptionValues.Text(Options, DerivedOptions, StreamOptionKey.EncryptionKeyFingerprint);
}

/// <summary>A removed stream. <see cref="RequestedOp" /> is the op of a deferred delete's request.</summary>
public sealed class MetadataStreamDeleted : MetadataChangeBody
{
    /// <summary>Stream id.</summary>
    public required uint StreamId { get; init; }

    /// <summary>The op that created the stream.</summary>
    public required ulong StreamCreatedOp { get; init; }

    /// <summary>Creation time in microseconds since the Unix epoch.</summary>
    public required ulong CreatedAt { get; init; }

    /// <summary>Stream name.</summary>
    public required string Name { get; init; }

    /// <summary>The op that requested a deferred delete, null when the delete completed at once.</summary>
    public ulong? RequestedOp { get; init; }
}

/// <summary>A topic after a create or update.</summary>
public sealed class MetadataTopicChange : MetadataChangeBody
{
    /// <summary>Stream id.</summary>
    public required uint StreamId { get; init; }

    /// <summary>The op that created the stream.</summary>
    public required ulong StreamCreatedOp { get; init; }

    /// <summary>Topic id, reused after a delete.</summary>
    public required uint TopicId { get; init; }

    /// <summary>The op that created the topic.</summary>
    public required ulong TopicCreatedOp { get; init; }

    /// <summary>Creation time in microseconds since the Unix epoch.</summary>
    public required ulong CreatedAt { get; init; }

    /// <summary>Topic name.</summary>
    public required string Name { get; init; }

    /// <summary>Partition count after the change.</summary>
    public required uint PartitionsCount { get; init; }

    /// <summary>Options a client set.</summary>
    public Dictionary<HeaderKey, HeaderValue>? Options { get; init; }

    /// <summary>Options the server filled from its defaults.</summary>
    public Dictionary<HeaderKey, HeaderValue>? DerivedOptions { get; init; }

    /// <summary>The server holds stored critical state it cannot read. A mirror keeps its previous values.</summary>
    public bool OptionsUnreadable { get; init; }
}

/// <summary>A topics item. Partition counts travel as partitions items.</summary>
public sealed class MetadataTopicItem : MetadataChangeBody
{
    /// <summary>Stream id.</summary>
    public required uint StreamId { get; init; }

    /// <summary>The op that created the stream.</summary>
    public required ulong StreamCreatedOp { get; init; }

    /// <summary>Topic id.</summary>
    public required uint TopicId { get; init; }

    /// <summary>The op that created the topic.</summary>
    public required ulong TopicCreatedOp { get; init; }

    /// <summary>Creation time in microseconds since the Unix epoch.</summary>
    public required ulong CreatedAt { get; init; }

    /// <summary>Topic name.</summary>
    public required string Name { get; init; }

    /// <summary>Options a client set.</summary>
    public Dictionary<HeaderKey, HeaderValue>? Options { get; init; }

    /// <summary>Options the server filled from its defaults.</summary>
    public Dictionary<HeaderKey, HeaderValue>? DerivedOptions { get; init; }

    /// <summary>The server holds stored critical state it cannot read.</summary>
    public bool OptionsUnreadable { get; init; }
}

/// <summary>A removed topic.</summary>
public sealed class MetadataTopicDeleted : MetadataChangeBody
{
    /// <summary>Stream id.</summary>
    public required uint StreamId { get; init; }

    /// <summary>The op that created the stream.</summary>
    public required ulong StreamCreatedOp { get; init; }

    /// <summary>Topic id.</summary>
    public required uint TopicId { get; init; }

    /// <summary>The op that created the topic.</summary>
    public required ulong TopicCreatedOp { get; init; }

    /// <summary>Creation time in microseconds since the Unix epoch.</summary>
    public required ulong CreatedAt { get; init; }

    /// <summary>Topic name.</summary>
    public required string Name { get; init; }

    /// <summary>The op that requested a deferred delete, null when the delete completed at once.</summary>
    public ulong? RequestedOp { get; init; }
}

/// <summary>
///     A partition count change. <see cref="RemovedPartitionIds" /> lists the deleted ids in ascending order and is
///     empty on create. Stored ids can be sparse, so the count is not an id boundary.
/// </summary>
public sealed class MetadataPartitionsChanged : MetadataChangeBody
{
    /// <summary>Stream id.</summary>
    public required uint StreamId { get; init; }

    /// <summary>The op that created the stream.</summary>
    public required ulong StreamCreatedOp { get; init; }

    /// <summary>Topic id.</summary>
    public required uint TopicId { get; init; }

    /// <summary>The op that created the topic.</summary>
    public required ulong TopicCreatedOp { get; init; }

    /// <summary>Partition count after the change.</summary>
    public required uint PartitionsCount { get; init; }

    /// <summary>Ids of the deleted partitions, ascending.</summary>
    public required IReadOnlyList<uint> RemovedPartitionIds { get; init; }

    /// <summary>The op that requested a deferred delete, null otherwise.</summary>
    public ulong? RequestedOp { get; init; }
}

/// <summary>A partitions item.</summary>
public sealed class MetadataPartitionsItem : MetadataChangeBody
{
    /// <summary>Stream id.</summary>
    public required uint StreamId { get; init; }

    /// <summary>The op that created the stream.</summary>
    public required ulong StreamCreatedOp { get; init; }

    /// <summary>Topic id.</summary>
    public required uint TopicId { get; init; }

    /// <summary>The op that created the topic.</summary>
    public required ulong TopicCreatedOp { get; init; }

    /// <summary>Partition count.</summary>
    public required uint PartitionsCount { get; init; }
}

/// <summary>A created consumer group with the topic's resulting group count.</summary>
public sealed class MetadataConsumerGroupCreated : MetadataChangeBody
{
    /// <summary>Stream id.</summary>
    public required uint StreamId { get; init; }

    /// <summary>The op that created the stream.</summary>
    public required ulong StreamCreatedOp { get; init; }

    /// <summary>Topic id.</summary>
    public required uint TopicId { get; init; }

    /// <summary>The op that created the topic.</summary>
    public required ulong TopicCreatedOp { get; init; }

    /// <summary>Consumer group id.</summary>
    public required uint GroupId { get; init; }

    /// <summary>Consumer group name.</summary>
    public required string Name { get; init; }

    /// <summary>The topic's consumer group count after the insertion.</summary>
    public required uint ConsumerGroupsCount { get; init; }
}

/// <summary>A removed consumer group with the topic's resulting group count.</summary>
public sealed class MetadataConsumerGroupDeleted : MetadataChangeBody
{
    /// <summary>Stream id.</summary>
    public required uint StreamId { get; init; }

    /// <summary>The op that created the stream.</summary>
    public required ulong StreamCreatedOp { get; init; }

    /// <summary>Topic id.</summary>
    public required uint TopicId { get; init; }

    /// <summary>The op that created the topic.</summary>
    public required ulong TopicCreatedOp { get; init; }

    /// <summary>Consumer group id.</summary>
    public required uint GroupId { get; init; }

    /// <summary>Consumer group name.</summary>
    public required string Name { get; init; }

    /// <summary>The topic's consumer group count after the removal.</summary>
    public required uint ConsumerGroupsCount { get; init; }

    /// <summary>The op that requested a deferred delete, null when the delete completed at once.</summary>
    public ulong? RequestedOp { get; init; }
}

/// <summary>A consumer groups item.</summary>
public sealed class MetadataConsumerGroupItem : MetadataChangeBody
{
    /// <summary>Stream id.</summary>
    public required uint StreamId { get; init; }

    /// <summary>The op that created the stream.</summary>
    public required ulong StreamCreatedOp { get; init; }

    /// <summary>Topic id.</summary>
    public required uint TopicId { get; init; }

    /// <summary>The op that created the topic.</summary>
    public required ulong TopicCreatedOp { get; init; }

    /// <summary>Consumer group id.</summary>
    public required uint GroupId { get; init; }

    /// <summary>Consumer group name.</summary>
    public required string Name { get; init; }
}

/// <summary>A member that joined, with its installed partitions, normally none because a handover installs them.</summary>
public sealed class MetadataMemberJoined : MetadataChangeBody
{
    /// <summary>Stream id.</summary>
    public required uint StreamId { get; init; }

    /// <summary>The op that created the stream.</summary>
    public required ulong StreamCreatedOp { get; init; }

    /// <summary>Topic id.</summary>
    public required uint TopicId { get; init; }

    /// <summary>The op that created the topic.</summary>
    public required ulong TopicCreatedOp { get; init; }

    /// <summary>Consumer group id.</summary>
    public required uint GroupId { get; init; }

    /// <summary>Member id within the group.</summary>
    public required uint MemberId { get; init; }

    /// <summary>The member's client id.</summary>
    public required UInt128 ClientId { get; init; }

    /// <summary>The group's member count after the join.</summary>
    public required uint MembersCount { get; init; }

    /// <summary>Installed partition ids, ascending.</summary>
    public required IReadOnlyList<uint> PartitionIds { get; init; }
}

/// <summary>A member that left the group.</summary>
public sealed class MetadataMemberLeft : MetadataChangeBody
{
    /// <summary>Stream id.</summary>
    public required uint StreamId { get; init; }

    /// <summary>The op that created the stream.</summary>
    public required ulong StreamCreatedOp { get; init; }

    /// <summary>Topic id.</summary>
    public required uint TopicId { get; init; }

    /// <summary>The op that created the topic.</summary>
    public required ulong TopicCreatedOp { get; init; }

    /// <summary>Consumer group id.</summary>
    public required uint GroupId { get; init; }

    /// <summary>Member id within the group.</summary>
    public required uint MemberId { get; init; }

    /// <summary>The member's client id.</summary>
    public required UInt128 ClientId { get; init; }

    /// <summary>The group's member count after the removal.</summary>
    public required uint MembersCount { get; init; }

    /// <summary>Why the member was removed.</summary>
    public required MetadataMemberLeftReason Reason { get; init; }
}

/// <summary>A completed partition handover. <see cref="MemberId" /> is null when the last member left.</summary>
public sealed class MetadataPartitionHandedOver : MetadataChangeBody
{
    /// <summary>Stream id.</summary>
    public required uint StreamId { get; init; }

    /// <summary>The op that created the stream.</summary>
    public required ulong StreamCreatedOp { get; init; }

    /// <summary>Topic id.</summary>
    public required uint TopicId { get; init; }

    /// <summary>The op that created the topic.</summary>
    public required ulong TopicCreatedOp { get; init; }

    /// <summary>Consumer group id.</summary>
    public required uint GroupId { get; init; }

    /// <summary>The partition handed over.</summary>
    public required uint PartitionId { get; init; }

    /// <summary>The new owner, null when nobody owns the partition.</summary>
    public uint? MemberId { get; init; }
}

/// <summary>A consumer group members item.</summary>
public sealed class MetadataMemberItem : MetadataChangeBody
{
    /// <summary>Stream id.</summary>
    public required uint StreamId { get; init; }

    /// <summary>The op that created the stream.</summary>
    public required ulong StreamCreatedOp { get; init; }

    /// <summary>Topic id.</summary>
    public required uint TopicId { get; init; }

    /// <summary>The op that created the topic.</summary>
    public required ulong TopicCreatedOp { get; init; }

    /// <summary>Consumer group id.</summary>
    public required uint GroupId { get; init; }

    /// <summary>Member id within the group.</summary>
    public required uint MemberId { get; init; }

    /// <summary>The member's client id.</summary>
    public required UInt128 ClientId { get; init; }

    /// <summary>Installed partition ids, ascending.</summary>
    public required IReadOnlyList<uint> PartitionIds { get; init; }
}

/// <summary>A user after a create or update, and a users item.</summary>
public sealed class MetadataUserChange : MetadataChangeBody
{
    /// <summary>User id, reused after a delete.</summary>
    public required uint UserId { get; init; }

    /// <summary>Creation time of the user in microseconds since the Unix epoch.</summary>
    public required ulong UserCreatedAt { get; init; }

    /// <summary>Username.</summary>
    public required string Username { get; init; }

    /// <summary>User status. An inactive user's sessions end.</summary>
    public required UserStatus Status { get; init; }

    /// <summary>Public permissions, null when the user has none.</summary>
    public Permissions? Permissions { get; init; }

    /// <summary>Options a client set.</summary>
    public Dictionary<HeaderKey, HeaderValue>? Options { get; init; }

    /// <summary>Options the server filled from its defaults.</summary>
    public Dictionary<HeaderKey, HeaderValue>? DerivedOptions { get; init; }

    /// <summary>The server holds stored critical state it cannot read.</summary>
    public bool OptionsUnreadable { get; init; }
}

/// <summary>A deleted user, or one whose password changed.</summary>
public sealed class MetadataUserIdentity : MetadataChangeBody
{
    /// <summary>User id.</summary>
    public required uint UserId { get; init; }

    /// <summary>Creation time of the user in microseconds since the Unix epoch.</summary>
    public required ulong UserCreatedAt { get; init; }

    /// <summary>Username.</summary>
    public required string Username { get; init; }
}

/// <summary>A user's public permission set after a change.</summary>
public sealed class MetadataUserPermissionsUpdated : MetadataChangeBody
{
    /// <summary>User id.</summary>
    public required uint UserId { get; init; }

    /// <summary>Creation time of the user in microseconds since the Unix epoch.</summary>
    public required ulong UserCreatedAt { get; init; }

    /// <summary>Username.</summary>
    public required string Username { get; init; }

    /// <summary>Public permissions after the change, null when the user has none.</summary>
    public Permissions? Permissions { get; init; }

    /// <summary>Why the set changed.</summary>
    public required MetadataPermissionsUpdatedReason Reason { get; init; }
}

/// <summary>A created personal access token, and a personal access tokens item. Never the token itself.</summary>
public sealed class MetadataTokenChange : MetadataChangeBody
{
    /// <summary>Owner id.</summary>
    public required uint UserId { get; init; }

    /// <summary>Creation time of the owner in microseconds since the Unix epoch.</summary>
    public required ulong UserCreatedAt { get; init; }

    /// <summary>Token name.</summary>
    public required string Name { get; init; }

    /// <summary>Expiry in microseconds since the Unix epoch, null for a token that never expires.</summary>
    public ulong? Expiry { get; init; }
}

/// <summary>A removed personal access token.</summary>
public sealed class MetadataTokenDeleted : MetadataChangeBody
{
    /// <summary>Owner id.</summary>
    public required uint UserId { get; init; }

    /// <summary>Creation time of the owner in microseconds since the Unix epoch.</summary>
    public required ulong UserCreatedAt { get; init; }

    /// <summary>Token name.</summary>
    public required string Name { get; init; }

    /// <summary>Why the token was removed.</summary>
    public required MetadataTokenDeletedReason Reason { get; init; }
}

/// <summary>A registered session, and a sessions item.</summary>
public sealed class MetadataSession : MetadataChangeBody
{
    /// <summary>User id.</summary>
    public required uint UserId { get; init; }

    /// <summary>Creation time of the user in microseconds since the Unix epoch.</summary>
    public required ulong UserCreatedAt { get; init; }

    /// <summary>The client id the session is bound to.</summary>
    public required UInt128 ClientId { get; init; }

    /// <summary>Session epoch.</summary>
    public required ulong Session { get; init; }
}

/// <summary>A closed session.</summary>
public sealed class MetadataSessionClosed : MetadataChangeBody
{
    /// <summary>User id.</summary>
    public required uint UserId { get; init; }

    /// <summary>Creation time of the user in microseconds since the Unix epoch.</summary>
    public required ulong UserCreatedAt { get; init; }

    /// <summary>The client id the session was bound to.</summary>
    public required UInt128 ClientId { get; init; }

    /// <summary>Session epoch.</summary>
    public required ulong Session { get; init; }

    /// <summary>Why the session was closed.</summary>
    public required MetadataSessionClosedReason Reason { get; init; }
}

/// <summary>
///     A binary record or item of a group or kind this client does not know, kept with its codes and raw body.
/// </summary>
public sealed class MetadataUnknownBody : MetadataChangeBody
{
    /// <summary>The group code.</summary>
    public required byte Group { get; init; }

    /// <summary>The kind code, null for an item.</summary>
    public byte? Kind { get; init; }

    /// <summary>The body exactly as the server sent it.</summary>
    public required byte[] Body { get; init; }
}

/// <summary>
///     A JSON record or item of a group or kind this client does not know, kept as the whole object.
/// </summary>
public sealed class MetadataUnknownJsonBody : MetadataChangeBody
{
    /// <summary>The group name.</summary>
    public required string Group { get; init; }

    /// <summary>The kind name, null for an item.</summary>
    public string? Kind { get; init; }

    /// <summary>The record or item object exactly as the server rendered it.</summary>
    public required string Json { get; init; }
}
