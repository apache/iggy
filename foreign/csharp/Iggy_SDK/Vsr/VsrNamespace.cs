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
using Apache.Iggy.Enums;
using Apache.Iggy.Utils;

namespace Apache.Iggy.Vsr;

/// <summary>
///     Packs a stream/topic/partition triple into the <c>RequestHeader.namespace</c> word the server hashes
///     to a shard. Mirrors <c>core/binary_protocol/src/namespace.rs</c>; any drift routes writes to the
///     wrong shard.
/// </summary>
internal static class VsrNamespace
{
    internal const int MAX_STREAMS = 4096;
    internal const int MAX_TOPICS = 4096;
    internal const int MAX_PARTITIONS = 1_000_000;

    internal const int PARTITION_BITS = 20;
    internal const int TOPIC_BITS = 12;
    internal const int STREAM_BITS = 12;

    internal const int PARTITION_SHIFT = 0;
    internal const int TOPIC_SHIFT = PARTITION_SHIFT + PARTITION_BITS;
    internal const int STREAM_SHIFT = TOPIC_SHIFT + TOPIC_BITS;

    internal const ulong PARTITION_MASK = (1UL << PARTITION_BITS) - 1;
    internal const ulong TOPIC_MASK = (1UL << TOPIC_BITS) - 1;
    internal const ulong STREAM_MASK = (1UL << STREAM_BITS) - 1;

    /// <summary>
    ///     Reserved value routing a request to the cluster's single metadata consensus group. Sits above the
    ///     packed layout, so it can never collide with a packed namespace.
    /// </summary>
    internal const ulong METADATA_CONSENSUS_NAMESPACE = 1UL << 63;

    private const byte NumericIdKind = 1;
    private const byte StringIdKind = 2;
    private const byte ConsumerKind = 1;
    private const byte ConsumerGroupKind = 2;

    /// <summary>
    ///     The namespace a request header carries for its operation. Register and logout ride the metadata
    ///     consensus group; non-replicated and metadata operations route by 0; partition-plane operations carry
    ///     the namespace the caller packed from its typed identifiers.
    /// </summary>
    internal static ulong ForOperation(VsrOperation operation, ulong partitionNamespace)
    {
        if (operation is VsrOperation.Register or VsrOperation.Logout)
        {
            return METADATA_CONSENSUS_NAMESPACE;
        }

        if (operation == VsrOperation.NonReplicated || operation.IsMetadata())
        {
            return 0;
        }

        return partitionNamespace;
    }

    /// <summary>
    ///     Packs the namespace for a partition-plane request from its typed identifiers. Named (string) stream
    ///     or topic identifiers resolve to 0; the server resolves them itself.
    /// </summary>
    internal static ulong ForPartition(Identifier streamId, Identifier topicId, uint partitionId)
    {
        return FromPartition(NumericValue(streamId), NumericValue(topicId), partitionId);
    }

    /// <summary>
    ///     The namespace for a raw binary request, peeking into the serialized body for the ops the server
    ///     routes by partition. Only <c>SendBinaryRequestAsync</c> lands here: the typed
    ///     command surface packs its namespace from typed identifiers instead.
    /// </summary>
    internal static ulong ForRequest(int code, ReadOnlySpan<byte> payload, VsrOperation operation)
    {
        if (operation is VsrOperation.Register or VsrOperation.Logout)
        {
            return METADATA_CONSENSUS_NAMESPACE;
        }

        if (operation == VsrOperation.NonReplicated || operation.IsMetadata())
        {
            return 0;
        }

        return code switch
        {
            CommandCodes.SEND_MESSAGES_CODE => FromSendMessages(payload),
            CommandCodes.STORE_CONSUMER_OFFSET_CODE or CommandCodes.DELETE_CONSUMER_OFFSET_CODE
                or CommandCodes.STORE_CONSUMER_OFFSET_2_CODE or CommandCodes.DELETE_CONSUMER_OFFSET_2_CODE =>
                FromConsumerOffset(payload),
            CommandCodes.DELETE_SEGMENTS_CODE => FromDeleteSegments(payload),
            _ => throw VsrError.Exception(VsrError.FEATURE_UNAVAILABLE,
                $"Command code {code} cannot be routed to a partition namespace.")
        };
    }

    internal static ulong Pack(uint streamId, uint topicId, uint partitionId)
    {
        return ((streamId & STREAM_MASK) << STREAM_SHIFT)
               | ((topicId & TOPIC_MASK) << TOPIC_SHIFT)
               | ((partitionId & PARTITION_MASK) << PARTITION_SHIFT);
    }

    /// <summary>
    ///     <c>[metadata_len u32][stream][topic][partitioning][messages_count u32]</c>. Only the metadata
    ///     prefix is peeked; the message batch that follows is never parsed.
    /// </summary>
    private static ulong FromSendMessages(ReadOnlySpan<byte> payload)
    {
        var metadataLength = (int)ReadUInt32(payload, 0);
        if (metadataLength < 0 || payload.Length - 4 < metadataLength)
        {
            throw Malformed();
        }

        ReadOnlySpan<byte> metadata = payload.Slice(4, metadataLength);
        var position = 0;
        var streamId = ReadIdentifier(metadata, ref position);
        var topicId = ReadIdentifier(metadata, ref position);

        var kind = ReadByte(metadata, position);
        var length = ReadByte(metadata, position + 1);
        position += 2;
        if (kind != (byte)Partitioning.PartitionId)
        {
            throw VsrError.Exception(VsrError.FEATURE_UNAVAILABLE,
                "Under VSR the partition must be resolved by the client; balanced and message-key partitioning cannot be sent on the wire.");
        }

        if (length != 4 || metadata.Length < position + 4)
        {
            throw Malformed();
        }

        return FromPartition(streamId, topicId, ReadUInt32(metadata, position));
    }

    /// <summary>
    ///     <c>[consumer kind u8][consumer id][stream][topic][partition flag u8][partition u32]</c>, shared by
    ///     the store and delete consumer-offset requests and their v2 variants.
    /// </summary>
    private static ulong FromConsumerOffset(ReadOnlySpan<byte> payload)
    {
        var consumerKind = ReadByte(payload, 0);
        if (consumerKind is not (ConsumerKind or ConsumerGroupKind))
        {
            throw Malformed();
        }

        var position = 1;
        _ = ReadIdentifier(payload, ref position);
        var streamId = ReadIdentifier(payload, ref position);
        var topicId = ReadIdentifier(payload, ref position);

        var flag = ReadByte(payload, position);
        position += 1;

        if (flag == 0)
        {
            throw VsrError.Exception(VsrError.INVALID_IDENTIFIER,
                "Under VSR a consumer-offset request must carry an explicit partition id.");
        }

        if (payload.Length < position + 4)
        {
            throw Malformed();
        }

        return FromPartition(streamId, topicId, ReadUInt32(payload, position));
    }

    /// <summary>
    ///     <c>[stream][topic][partition u32][segments_count u32]</c>.
    /// </summary>
    private static ulong FromDeleteSegments(ReadOnlySpan<byte> payload)
    {
        var position = 0;
        var streamId = ReadIdentifier(payload, ref position);
        var topicId = ReadIdentifier(payload, ref position);
        if (payload.Length < position + 4)
        {
            throw Malformed();
        }

        return FromPartition(streamId, topicId, ReadUInt32(payload, position));
    }

    /// <summary>Numeric identifier value, or <c>null</c> for a named identifier the server has to resolve.</summary>
    private static uint? NumericValue(Identifier identifier)
    {
        if (identifier.Kind != IdKind.Numeric)
        {
            return null;
        }

        if (identifier.Length != 4 || identifier.Value.Length < 4)
        {
            throw Malformed();
        }

        return BinaryPrimitives.ReadUInt32LittleEndian(identifier.Value);
    }

    private static ulong FromPartition(uint? streamId, uint? topicId, uint partitionId)
    {
        if (streamId is null || topicId is null)
        {
            return 0;
        }

        Validate(streamId.Value, MAX_STREAMS);
        Validate(topicId.Value, MAX_TOPICS);
        Validate(partitionId, MAX_PARTITIONS);

        return Pack(streamId.Value, topicId.Value, partitionId);
    }

    private static void Validate(uint value, int exclusiveMax)
    {
        if (value >= exclusiveMax)
        {
            throw VsrError.Exception(VsrError.INVALID_IDENTIFIER,
                $"Identifier {value} is outside the packable namespace range (max {exclusiveMax - 1}).");
        }
    }

    /// <summary>
    ///     Reads an <c>[kind u8][len u8][value]</c> identifier, advancing <paramref name="position" />.
    ///     Returns the numeric value, or <c>null</c> for a named identifier the server has to resolve.
    ///     A kind or width the wire format does not define is corruption, not a named identifier: falling
    ///     back to <c>null</c> would route the request to namespace 0 instead of failing it.
    /// </summary>
    private static uint? ReadIdentifier(ReadOnlySpan<byte> payload, ref int position)
    {
        var kind = ReadByte(payload, position);
        var length = ReadByte(payload, position + 1);
        if (payload.Length < position + 2 + length)
        {
            throw Malformed();
        }

        uint? value = (kind, length) switch
        {
            (NumericIdKind, 4) => ReadUInt32(payload, position + 2),
            (StringIdKind, > 0) => null,
            _ => throw Malformed()
        };
        position += 2 + length;

        return value;
    }

    private static byte ReadByte(ReadOnlySpan<byte> payload, int offset)
    {
        if (payload.Length <= offset)
        {
            throw Malformed();
        }

        return payload[offset];
    }

    private static uint ReadUInt32(ReadOnlySpan<byte> payload, int offset)
    {
        if (payload.Length < offset + 4)
        {
            throw Malformed();
        }

        return BinaryPrimitives.ReadUInt32LittleEndian(payload.Slice(offset, 4));
    }

    private static Exception Malformed()
    {
        return VsrError.Exception(VsrError.INVALID_COMMAND, "Request payload is too short to resolve a namespace.");
    }
}
