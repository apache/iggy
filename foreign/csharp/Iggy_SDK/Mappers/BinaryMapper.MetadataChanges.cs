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

using System.Buffers.Binary;
using System.Text;
using Apache.Iggy.Contracts;
using Apache.Iggy.Contracts.Auth;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Headers;

namespace Apache.Iggy.Mappers;

internal static partial class BinaryMapper
{
    private const int METADATA_RECORD_MIN_SIZE = 8 + 4 + 16 + 8 + 1 + 1 + 1 + 4;
    private const int METADATA_ITEM_MIN_SIZE = 1 + 4;

    /// <summary>
    ///     Maps a metadata changes answer. Bodies are decoded strictly: unknown reason codes, a known kind under the
    ///     wrong group, presence bytes other than 0 and 1 and trailing bytes are refused, while a group or kind this
    ///     build does not know is kept with its raw body.
    /// </summary>
    internal static MetadataChanges MapMetadataChanges(ReadOnlySpan<byte> payload)
    {
        var reader = new WireReader(payload);
        var status = (MetadataChangeStatus)reader.CodeAtMost((byte)MetadataChangeStatus.Reset, "status");
        var phase = (MetadataChangePhase)reader.CodeAtMost((byte)MetadataChangePhase.Current, "phase");
        UInt128? logId = reader.Flag("log id presence") ? reader.U128() : null;
        var oldestOp = reader.U64();
        var retainedSince = reader.U64();
        var headOp = reader.U64();
        MetadataChangeCursor? next = reader.Flag("next cursor presence") ? reader.Cursor() : null;
        var hasMore = reader.Flag("has more");

        var records = new MetadataChangeRecord[ValidatedCollectionSize(reader.U32(), reader.Remaining,
            METADATA_RECORD_MIN_SIZE, "Metadata change records count")];
        for (var index = 0; index < records.Length; index++)
        {
            records[index] = ReadChangeRecord(ref reader);
        }

        var items = new MetadataCurrentItem[ValidatedCollectionSize(reader.U32(), reader.Remaining,
            METADATA_ITEM_MIN_SIZE, "Metadata current items count")];
        for (var index = 0; index < items.Length; index++)
        {
            items[index] = ReadCurrentItem(ref reader);
        }

        MetadataCurrentPageToken? pageToken = null;
        var tokenLength = reader.U16();
        if (tokenLength > 0 && !MetadataCurrentPageToken.TryDecode(reader.Bytes(tokenLength), out pageToken))
        {
            throw new MalformedResponseException("Metadata changes answer carries a malformed page token.");
        }

        var holdsOtherPhase = phase == MetadataChangePhase.Changes
            ? items.Length > 0 || pageToken is not null
            : records.Length > 0;
        if (holdsOtherPhase)
        {
            throw new MalformedResponseException(
                "Metadata changes answer mixes phases: a changes page carries only records, a current page only items.");
        }

        return new MetadataChanges
        {
            Status = status,
            Phase = phase,
            LogId = logId,
            OldestOp = oldestOp,
            RetainedSince = retainedSince,
            HeadOp = headOp,
            Next = next,
            HasMore = hasMore,
            Records = records,
            Items = items,
            CurrentPageToken = pageToken
        };
    }

    /// <summary>Maps exactly one change record envelope with its body.</summary>
    internal static MetadataChangeRecord MapMetadataChangeRecord(ReadOnlySpan<byte> payload)
    {
        var reader = new WireReader(payload);
        var record = ReadChangeRecord(ref reader);
        reader.EnsureConsumed("change record");
        return record;
    }

    /// <summary>Maps exactly one current state item envelope with its body.</summary>
    internal static MetadataCurrentItem MapMetadataCurrentItem(ReadOnlySpan<byte> payload)
    {
        var reader = new WireReader(payload);
        var item = ReadCurrentItem(ref reader);
        reader.EnsureConsumed("current item");
        return item;
    }

    /// <summary>
    ///     Decodes a change body under its group and kind codes. A group or kind this build does not know yields
    ///     <see cref="MetadataUnknownBody" /> and a null kind.
    /// </summary>
    internal static MetadataChangeBody MapMetadataChangeBody(byte group, byte kind, ReadOnlySpan<byte> body,
        out MetadataChangeKind? knownKind)
    {
        knownKind = null;
        var changeGroup = (MetadataChangeGroup)group;
        var changeKind = (MetadataChangeKind)kind;
        if (!MetadataChangeGroups.IsDefined(changeGroup) || !MetadataChangeGroups.IsDefined(changeKind))
        {
            return new MetadataUnknownBody { Group = group, Kind = kind, Body = body.ToArray() };
        }

        if (MetadataChangeGroups.Of(changeKind) != changeGroup)
        {
            throw new MalformedResponseException($"Metadata change kind {kind} does not belong to group {group}.");
        }

        knownKind = changeKind;
        var reader = new WireReader(body);
        MetadataChangeBody change = changeKind switch
        {
            MetadataChangeKind.StreamCreated or MetadataChangeKind.StreamUpdated => ReadStream(ref reader),
            MetadataChangeKind.StreamDeleted => ReadStreamDeleted(ref reader),
            MetadataChangeKind.TopicCreated or MetadataChangeKind.TopicUpdated => ReadTopic(ref reader),
            MetadataChangeKind.TopicDeleted => ReadTopicDeleted(ref reader),
            MetadataChangeKind.PartitionsChanged => ReadPartitionsChanged(ref reader),
            MetadataChangeKind.ConsumerGroupCreated => ReadConsumerGroupCreated(ref reader),
            MetadataChangeKind.ConsumerGroupDeleted => ReadConsumerGroupDeleted(ref reader),
            MetadataChangeKind.ConsumerGroupMemberJoined => ReadMemberJoined(ref reader),
            MetadataChangeKind.ConsumerGroupMemberLeft => ReadMemberLeft(ref reader),
            MetadataChangeKind.ConsumerGroupPartitionHandedOver => ReadPartitionHandedOver(ref reader),
            MetadataChangeKind.UserCreated or MetadataChangeKind.UserUpdated => ReadUser(ref reader),
            MetadataChangeKind.UserDeleted or MetadataChangeKind.UserPasswordChanged => ReadUserIdentity(ref reader),
            MetadataChangeKind.UserPermissionsUpdated => ReadUserPermissions(ref reader),
            MetadataChangeKind.PersonalAccessTokenCreated => ReadToken(ref reader),
            MetadataChangeKind.PersonalAccessTokenDeleted => ReadTokenDeleted(ref reader),
            MetadataChangeKind.SessionRegistered => ReadSession(ref reader),
            MetadataChangeKind.SessionClosed => ReadSessionClosed(ref reader),
            _ => throw new MalformedResponseException($"Metadata change kind {kind} has no decoder.")
        };
        reader.EnsureConsumed("change body");
        return change;
    }

    /// <summary>
    ///     Decodes a current state item body under its group code. A group this build does not know yields
    ///     <see cref="MetadataUnknownBody" /> and a null group.
    /// </summary>
    internal static MetadataChangeBody MapMetadataItemBody(byte group, ReadOnlySpan<byte> body,
        out MetadataChangeGroup? knownGroup)
    {
        knownGroup = null;
        var itemGroup = (MetadataChangeGroup)group;
        if (!MetadataChangeGroups.IsDefined(itemGroup))
        {
            return new MetadataUnknownBody { Group = group, Body = body.ToArray() };
        }

        knownGroup = itemGroup;
        var reader = new WireReader(body);
        MetadataChangeBody item = itemGroup switch
        {
            MetadataChangeGroup.Streams => ReadStream(ref reader),
            MetadataChangeGroup.Topics => ReadTopicItem(ref reader),
            MetadataChangeGroup.Partitions => ReadPartitionsItem(ref reader),
            MetadataChangeGroup.ConsumerGroups => ReadConsumerGroupItem(ref reader),
            MetadataChangeGroup.ConsumerGroupMembers => ReadMemberItem(ref reader),
            MetadataChangeGroup.Users => ReadUser(ref reader),
            MetadataChangeGroup.PersonalAccessTokens => ReadToken(ref reader),
            MetadataChangeGroup.Sessions => ReadSession(ref reader),
            _ => throw new MalformedResponseException($"Metadata current item group {group} has no decoder.")
        };
        reader.EnsureConsumed("current item body");
        return item;
    }

    private static MetadataChangeRecord ReadChangeRecord(ref WireReader reader)
    {
        var op = reader.U64();
        var ordinal = reader.U32();
        var checksum = reader.U128();
        var timestamp = reader.U64();
        uint? actorUserId = reader.Flag("actor presence") ? reader.U32() : null;
        var group = reader.U8();
        var kind = reader.U8();
        var body = MapMetadataChangeBody(group, kind, reader.Bytes(reader.U32()), out var knownKind);
        return new MetadataChangeRecord
        {
            Op = op,
            Ordinal = ordinal,
            Checksum = checksum,
            Timestamp = timestamp,
            ActorUserId = actorUserId,
            Kind = knownKind,
            Body = body
        };
    }

    private static MetadataCurrentItem ReadCurrentItem(ref WireReader reader)
    {
        var group = reader.U8();
        var body = MapMetadataItemBody(group, reader.Bytes(reader.U32()), out var knownGroup);
        return new MetadataCurrentItem { Group = knownGroup, Body = body };
    }

    private static MetadataStreamChange ReadStream(ref WireReader reader)
    {
        var streamId = reader.U32();
        var streamCreatedOp = reader.U64();
        var createdAt = reader.U64();
        var name = reader.Name();
        var unreadable = ReadChangeOptions(ref reader, out var options, out var derivedOptions);
        return new MetadataStreamChange
        {
            StreamId = streamId,
            StreamCreatedOp = streamCreatedOp,
            CreatedAt = createdAt,
            Name = name,
            Options = options,
            DerivedOptions = derivedOptions,
            OptionsUnreadable = unreadable
        };
    }

    private static MetadataStreamDeleted ReadStreamDeleted(ref WireReader reader)
    {
        return new MetadataStreamDeleted
        {
            StreamId = reader.U32(),
            StreamCreatedOp = reader.U64(),
            CreatedAt = reader.U64(),
            Name = reader.Name(),
            RequestedOp = reader.OptionalU64("requested op presence")
        };
    }

    private static MetadataTopicChange ReadTopic(ref WireReader reader)
    {
        var topic = ReadTopicKey(ref reader);
        var createdAt = reader.U64();
        var name = reader.Name();
        var partitionsCount = reader.U32();
        var unreadable = ReadChangeOptions(ref reader, out var options, out var derivedOptions);
        return new MetadataTopicChange
        {
            StreamId = topic.StreamId,
            StreamCreatedOp = topic.StreamCreatedOp,
            TopicId = topic.TopicId,
            TopicCreatedOp = topic.TopicCreatedOp,
            CreatedAt = createdAt,
            Name = name,
            PartitionsCount = partitionsCount,
            Options = options,
            DerivedOptions = derivedOptions,
            OptionsUnreadable = unreadable
        };
    }

    private static MetadataTopicItem ReadTopicItem(ref WireReader reader)
    {
        var topic = ReadTopicKey(ref reader);
        var createdAt = reader.U64();
        var name = reader.Name();
        var unreadable = ReadChangeOptions(ref reader, out var options, out var derivedOptions);
        return new MetadataTopicItem
        {
            StreamId = topic.StreamId,
            StreamCreatedOp = topic.StreamCreatedOp,
            TopicId = topic.TopicId,
            TopicCreatedOp = topic.TopicCreatedOp,
            CreatedAt = createdAt,
            Name = name,
            Options = options,
            DerivedOptions = derivedOptions,
            OptionsUnreadable = unreadable
        };
    }

    private static MetadataTopicDeleted ReadTopicDeleted(ref WireReader reader)
    {
        var topic = ReadTopicKey(ref reader);
        return new MetadataTopicDeleted
        {
            StreamId = topic.StreamId,
            StreamCreatedOp = topic.StreamCreatedOp,
            TopicId = topic.TopicId,
            TopicCreatedOp = topic.TopicCreatedOp,
            CreatedAt = reader.U64(),
            Name = reader.Name(),
            RequestedOp = reader.OptionalU64("requested op presence")
        };
    }

    private static MetadataPartitionsChanged ReadPartitionsChanged(ref WireReader reader)
    {
        var topic = ReadTopicKey(ref reader);
        return new MetadataPartitionsChanged
        {
            StreamId = topic.StreamId,
            StreamCreatedOp = topic.StreamCreatedOp,
            TopicId = topic.TopicId,
            TopicCreatedOp = topic.TopicCreatedOp,
            PartitionsCount = reader.U32(),
            RemovedPartitionIds = reader.PartitionIds(),
            RequestedOp = reader.OptionalU64("requested op presence")
        };
    }

    private static MetadataPartitionsItem ReadPartitionsItem(ref WireReader reader)
    {
        var topic = ReadTopicKey(ref reader);
        return new MetadataPartitionsItem
        {
            StreamId = topic.StreamId,
            StreamCreatedOp = topic.StreamCreatedOp,
            TopicId = topic.TopicId,
            TopicCreatedOp = topic.TopicCreatedOp,
            PartitionsCount = reader.U32()
        };
    }

    private static MetadataConsumerGroupCreated ReadConsumerGroupCreated(ref WireReader reader)
    {
        var topic = ReadTopicKey(ref reader);
        return new MetadataConsumerGroupCreated
        {
            StreamId = topic.StreamId,
            StreamCreatedOp = topic.StreamCreatedOp,
            TopicId = topic.TopicId,
            TopicCreatedOp = topic.TopicCreatedOp,
            GroupId = reader.U32(),
            Name = reader.Name(),
            ConsumerGroupsCount = reader.U32()
        };
    }

    private static MetadataConsumerGroupDeleted ReadConsumerGroupDeleted(ref WireReader reader)
    {
        var topic = ReadTopicKey(ref reader);
        return new MetadataConsumerGroupDeleted
        {
            StreamId = topic.StreamId,
            StreamCreatedOp = topic.StreamCreatedOp,
            TopicId = topic.TopicId,
            TopicCreatedOp = topic.TopicCreatedOp,
            GroupId = reader.U32(),
            Name = reader.Name(),
            ConsumerGroupsCount = reader.U32(),
            RequestedOp = reader.OptionalU64("requested op presence")
        };
    }

    private static MetadataConsumerGroupItem ReadConsumerGroupItem(ref WireReader reader)
    {
        var topic = ReadTopicKey(ref reader);
        return new MetadataConsumerGroupItem
        {
            StreamId = topic.StreamId,
            StreamCreatedOp = topic.StreamCreatedOp,
            TopicId = topic.TopicId,
            TopicCreatedOp = topic.TopicCreatedOp,
            GroupId = reader.U32(),
            Name = reader.Name()
        };
    }

    private static MetadataMemberJoined ReadMemberJoined(ref WireReader reader)
    {
        var topic = ReadTopicKey(ref reader);
        return new MetadataMemberJoined
        {
            StreamId = topic.StreamId,
            StreamCreatedOp = topic.StreamCreatedOp,
            TopicId = topic.TopicId,
            TopicCreatedOp = topic.TopicCreatedOp,
            GroupId = reader.U32(),
            MemberId = reader.U32(),
            ClientId = reader.U128(),
            MembersCount = reader.U32(),
            PartitionIds = reader.PartitionIds()
        };
    }

    private static MetadataMemberLeft ReadMemberLeft(ref WireReader reader)
    {
        var topic = ReadTopicKey(ref reader);
        return new MetadataMemberLeft
        {
            StreamId = topic.StreamId,
            StreamCreatedOp = topic.StreamCreatedOp,
            TopicId = topic.TopicId,
            TopicCreatedOp = topic.TopicCreatedOp,
            GroupId = reader.U32(),
            MemberId = reader.U32(),
            ClientId = reader.U128(),
            MembersCount = reader.U32(),
            Reason = (MetadataMemberLeftReason)reader.CodeAtMost((byte)MetadataMemberLeftReason.SessionEnded,
                "member left reason")
        };
    }

    private static MetadataPartitionHandedOver ReadPartitionHandedOver(ref WireReader reader)
    {
        var topic = ReadTopicKey(ref reader);
        return new MetadataPartitionHandedOver
        {
            StreamId = topic.StreamId,
            StreamCreatedOp = topic.StreamCreatedOp,
            TopicId = topic.TopicId,
            TopicCreatedOp = topic.TopicCreatedOp,
            GroupId = reader.U32(),
            PartitionId = reader.U32(),
            MemberId = reader.Flag("member presence") ? reader.U32() : null
        };
    }

    private static MetadataMemberItem ReadMemberItem(ref WireReader reader)
    {
        var topic = ReadTopicKey(ref reader);
        return new MetadataMemberItem
        {
            StreamId = topic.StreamId,
            StreamCreatedOp = topic.StreamCreatedOp,
            TopicId = topic.TopicId,
            TopicCreatedOp = topic.TopicCreatedOp,
            GroupId = reader.U32(),
            MemberId = reader.U32(),
            ClientId = reader.U128(),
            PartitionIds = reader.PartitionIds()
        };
    }

    private static MetadataUserChange ReadUser(ref WireReader reader)
    {
        var userId = reader.U32();
        var userCreatedAt = reader.U64();
        var username = reader.Name();
        var status = reader.U8() switch
        {
            1 => UserStatus.Active,
            2 => UserStatus.Inactive,
            var code => throw new MalformedResponseException($"Metadata change user status {code} is unknown.")
        };
        var permissions = reader.Flag("permissions presence") ? reader.Permissions() : null;
        var unreadable = ReadChangeOptions(ref reader, out var options, out var derivedOptions);
        return new MetadataUserChange
        {
            UserId = userId,
            UserCreatedAt = userCreatedAt,
            Username = username,
            Status = status,
            Permissions = permissions,
            Options = options,
            DerivedOptions = derivedOptions,
            OptionsUnreadable = unreadable
        };
    }

    private static MetadataUserIdentity ReadUserIdentity(ref WireReader reader)
    {
        return new MetadataUserIdentity
        {
            UserId = reader.U32(),
            UserCreatedAt = reader.U64(),
            Username = reader.Name()
        };
    }

    private static MetadataUserPermissionsUpdated ReadUserPermissions(ref WireReader reader)
    {
        return new MetadataUserPermissionsUpdated
        {
            UserId = reader.U32(),
            UserCreatedAt = reader.U64(),
            Username = reader.Name(),
            Permissions = reader.Flag("permissions presence") ? reader.Permissions() : null,
            Reason = (MetadataPermissionsUpdatedReason)reader.CodeAtMost(
                (byte)MetadataPermissionsUpdatedReason.ResourceDeleted, "permissions updated reason")
        };
    }

    private static MetadataTokenChange ReadToken(ref WireReader reader)
    {
        var userId = reader.U32();
        var userCreatedAt = reader.U64();
        var name = reader.Name();
        var expiry = reader.U64();
        return new MetadataTokenChange
        {
            UserId = userId,
            UserCreatedAt = userCreatedAt,
            Name = name,
            Expiry = expiry == 0 ? null : expiry
        };
    }

    private static MetadataTokenDeleted ReadTokenDeleted(ref WireReader reader)
    {
        return new MetadataTokenDeleted
        {
            UserId = reader.U32(),
            UserCreatedAt = reader.U64(),
            Name = reader.Name(),
            Reason = (MetadataTokenDeletedReason)reader.CodeAtMost((byte)MetadataTokenDeletedReason.Expired,
                "token deleted reason")
        };
    }

    private static MetadataSession ReadSession(ref WireReader reader)
    {
        return new MetadataSession
        {
            UserId = reader.U32(),
            UserCreatedAt = reader.U64(),
            ClientId = reader.U128(),
            Session = reader.U64()
        };
    }

    private static MetadataSessionClosed ReadSessionClosed(ref WireReader reader)
    {
        return new MetadataSessionClosed
        {
            UserId = reader.U32(),
            UserCreatedAt = reader.U64(),
            ClientId = reader.U128(),
            Session = reader.U64(),
            Reason = (MetadataSessionClosedReason)reader.CodeAtMost((byte)MetadataSessionClosedReason.Revoked,
                "session closed reason")
        };
    }

    private static (uint StreamId, ulong StreamCreatedOp, uint TopicId, ulong TopicCreatedOp) ReadTopicKey(
        ref WireReader reader)
    {
        return (reader.U32(), reader.U64(), reader.U32(), reader.U64());
    }

    /// <summary>
    ///     Reads <c>[state:u8]</c>: 0 followed by the explicit and derived option blocks exactly as the read
    ///     responses carry them, or 1 for stored state the server cannot read. Returns whether it was unreadable.
    /// </summary>
    private static bool ReadChangeOptions(ref WireReader reader, out Dictionary<HeaderKey, HeaderValue>? options,
        out Dictionary<HeaderKey, HeaderValue>? derivedOptions)
    {
        options = null;
        derivedOptions = null;
        var state = reader.CodeAtMost(1, "options state");
        if (state == 1)
        {
            return true;
        }

        options = MapOptions(reader.Buffer, reader.Position, out var explicitBytes);
        reader.Skip(explicitBytes);
        derivedOptions = MapOptions(reader.Buffer, reader.Position, out var derivedBytes);
        reader.Skip(derivedBytes);
        return false;
    }

    /// <summary>
    ///     A bounds-checked little-endian cursor over a metadata changes answer or a user's permission set. Every
    ///     overrun is a <see cref="MalformedResponseException" /> instead of a slicing exception.
    /// </summary>
    private ref struct WireReader
    {
        private static readonly UTF8Encoding StrictUtf8 = new(false, true);

        internal WireReader(ReadOnlySpan<byte> buffer)
        {
            Buffer = buffer;
        }

        internal ReadOnlySpan<byte> Buffer { get; }

        internal int Position { get; private set; }

        internal readonly int Remaining => Buffer.Length - Position;

        internal ReadOnlySpan<byte> Bytes(uint length)
        {
            if (length > Remaining)
            {
                throw new MalformedResponseException(
                    $"Response field of {length} bytes at byte {Position} overruns the {Buffer.Length}-byte payload.");
            }

            var bytes = Buffer.Slice(Position, (int)length);
            Position += (int)length;
            return bytes;
        }

        internal void Skip(int length)
        {
            Bytes((uint)length);
        }

        internal byte U8()
        {
            return Bytes(1)[0];
        }

        internal ushort U16()
        {
            return BinaryPrimitives.ReadUInt16LittleEndian(Bytes(2));
        }

        internal uint U32()
        {
            return BinaryPrimitives.ReadUInt32LittleEndian(Bytes(4));
        }

        internal ulong U64()
        {
            return BinaryPrimitives.ReadUInt64LittleEndian(Bytes(8));
        }

        internal UInt128 U128()
        {
            return BinaryPrimitives.ReadUInt128LittleEndian(Bytes(16));
        }

        internal byte CodeAtMost(byte maximum, string field)
        {
            var offset = Position;
            var code = U8();
            return code <= maximum
                ? code
                : throw new MalformedResponseException($"Metadata changes {field} {code} at byte {offset} is unknown.");
        }

        internal bool Flag(string field)
        {
            return CodeAtMost(1, field) == 1;
        }

        internal ulong? OptionalU64(string field)
        {
            return Flag(field) ? U64() : null;
        }

        internal string Name()
        {
            var offset = Position;
            var length = U8();
            if (length == 0)
            {
                throw new MalformedResponseException($"Metadata changes name at byte {offset} is empty.");
            }

            try
            {
                return StrictUtf8.GetString(Bytes(length));
            }
            catch (DecoderFallbackException)
            {
                throw new MalformedResponseException($"Metadata changes name at byte {offset} is not UTF-8.");
            }
        }

        internal MetadataChangeCursor Cursor()
        {
            var cursor = MetadataChangeCursor.Read(Bytes(MetadataChangeCursor.SIZE));
            return cursor.IsCanonical
                ? cursor
                : throw new MalformedResponseException("Metadata changes cursor at op 0 carries a checksum or ordinal.");
        }

        /// <summary>Reads <c>[count:u32][id:u32]*</c>, refusing ids that are not strictly ascending.</summary>
        internal uint[] PartitionIds()
        {
            var ids = new uint[ValidatedCollectionSize(U32(), Remaining, 4, "Metadata changes partition ids count")];
            for (var index = 0; index < ids.Length; index++)
            {
                ids[index] = U32();
                if (index > 0 && ids[index] <= ids[index - 1])
                {
                    throw new MalformedResponseException("Metadata changes partition ids must be strictly ascending.");
                }
            }

            return ids;
        }

        /// <summary>
        ///     Reads the public permission set: ten global flags, then streams and their topics, each list
        ///     continued while its next flag is set.
        /// </summary>
        internal Permissions Permissions()
        {
            var global = new GlobalPermissions
            {
                ManageServers = U8() != 0,
                ReadServers = U8() != 0,
                ManageUsers = U8() != 0,
                ReadUsers = U8() != 0,
                ManageStreams = U8() != 0,
                ReadStreams = U8() != 0,
                ManageTopics = U8() != 0,
                ReadTopics = U8() != 0,
                PollMessages = U8() != 0,
                SendMessages = U8() != 0
            };

            Dictionary<uint, StreamPermissions>? streams = null;
            var hasStream = U8() != 0;
            while (hasStream)
            {
                var streamId = U32();
                var manageStream = U8() != 0;
                var readStream = U8() != 0;
                var manageTopics = U8() != 0;
                var readTopics = U8() != 0;
                var pollMessages = U8() != 0;
                var sendMessages = U8() != 0;
                Dictionary<uint, TopicPermissions>? topics = null;
                var hasTopic = U8() != 0;
                while (hasTopic)
                {
                    topics ??= new Dictionary<uint, TopicPermissions>();
                    topics[U32()] = new TopicPermissions
                    {
                        ManageTopic = U8() != 0,
                        ReadTopic = U8() != 0,
                        PollMessages = U8() != 0,
                        SendMessages = U8() != 0
                    };
                    hasTopic = U8() != 0;
                }

                streams ??= new Dictionary<uint, StreamPermissions>();
                streams[streamId] = new StreamPermissions
                {
                    ManageStream = manageStream,
                    ReadStream = readStream,
                    ManageTopics = manageTopics,
                    ReadTopics = readTopics,
                    PollMessages = pollMessages,
                    SendMessages = sendMessages,
                    Topics = topics
                };
                hasStream = U8() != 0;
            }

            return new Permissions { Global = global, Streams = streams };
        }

        internal readonly void EnsureConsumed(string what)
        {
            if (Remaining != 0)
            {
                throw new MalformedResponseException(
                    $"Metadata changes {what} has {Remaining} trailing bytes after byte {Position}.");
            }
        }
    }
}
