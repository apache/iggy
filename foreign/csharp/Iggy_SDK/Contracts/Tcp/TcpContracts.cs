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

using System.Buffers;
using System.Buffers.Binary;
using System.IO.Hashing;
using System.Runtime.CompilerServices;
using System.Text;
using Apache.Iggy.Contracts.Auth;
using Apache.Iggy.Encryption;
using Apache.Iggy.Enums;
using Apache.Iggy.Extensions;
using Apache.Iggy.Headers;
using Apache.Iggy.Kinds;
using Apache.Iggy.Messages;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.Contracts.Tcp;

internal static class TcpContracts
{
    private const int MaxWireNameLength = 255;

    /// <summary>Frames wider than this are built on the heap instead of the stack.</summary>
    private const int MaxStackAllocBytes = 1024;

    /// <summary>Offset mutations always request quorum acknowledgement.</summary>
    private const byte AckQuorum = 1;

    internal static byte[] LoginWithPersonalAccessToken(string token)
    {
        var tokenLength = Encoding.UTF8.GetByteCount(token);
        Span<byte> bytes = stackalloc byte[5 + tokenLength];
        bytes[0] = (byte)tokenLength;
        Encoding.UTF8.GetBytes(token, bytes[1..(1 + tokenLength)]);
        return bytes.ToArray();
    }

    internal static byte[] DeletePersonalRequestToken(string name)
    {
        var nameLength = Encoding.UTF8.GetByteCount(name);
        Span<byte> bytes = stackalloc byte[5 + nameLength];
        bytes[0] = (byte)nameLength;
        Encoding.UTF8.GetBytes(name, bytes[1..(1 + nameLength)]);
        return bytes.ToArray();
    }

    internal static byte[] CreatePersonalAccessToken(string name, ulong? expiry)
    {
        var nameLength = Encoding.UTF8.GetByteCount(name);
        Span<byte> bytes = stackalloc byte[1 + nameLength + 8];
        bytes[0] = (byte)nameLength;
        Encoding.UTF8.GetBytes(name, bytes[1..(1 + nameLength)]);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes[(1 + nameLength)..], expiry ?? 0);
        return bytes.ToArray();
    }

    internal static byte[] GetClient(uint clientId)
    {
        var bytes = new byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(bytes, clientId);
        return bytes;
    }

    internal static byte[] GetUser(Identifier userId)
    {
        Span<byte> bytes = stackalloc byte[userId.Length + 2];
        bytes.WriteBytesFromIdentifier(userId);
        return bytes.ToArray();
    }

    internal static byte[] DeleteUser(Identifier userId)
    {
        Span<byte> bytes = stackalloc byte[userId.Length + 2];
        bytes.WriteBytesFromIdentifier(userId);
        return bytes.ToArray();
    }

    internal static byte[] LoginUser(string userName, string password, string? version, string? context)
    {
        var bytes = new List<byte>();

        var usernameBytes = Encoding.UTF8.GetBytes(userName);
        bytes.Add((byte)usernameBytes.Length);
        bytes.AddRange(usernameBytes);

        var passwordBytes = Encoding.UTF8.GetBytes(password);
        bytes.Add((byte)passwordBytes.Length);
        bytes.AddRange(passwordBytes);

        if (!string.IsNullOrEmpty(version))
        {
            var versionBytes = Encoding.UTF8.GetBytes(version);
            bytes.AddRange(BitConverter.GetBytes(versionBytes.Length));
            bytes.AddRange(versionBytes);
        }
        else
        {
            bytes.AddRange(BitConverter.GetBytes(0));
        }

        if (!string.IsNullOrEmpty(context))
        {
            var contextBytes = Encoding.UTF8.GetBytes(context);
            bytes.AddRange(BitConverter.GetBytes(contextBytes.Length));
            bytes.AddRange(contextBytes);
        }
        else
        {
            bytes.AddRange(BitConverter.GetBytes(0));
        }

        return bytes.ToArray();
    }

    internal static byte[] ChangePassword(Identifier userId, string currentPassword, string newPassword)
    {
        var currentPasswordLength = Encoding.UTF8.GetByteCount(currentPassword);
        var newPasswordLength = Encoding.UTF8.GetByteCount(newPassword);
        var length = userId.Length + 2 + currentPasswordLength + newPasswordLength + 2;
        Span<byte> bytes = stackalloc byte[length];

        bytes.WriteBytesFromIdentifier(userId);
        var position = userId.Length + 2;
        bytes[position] = (byte)currentPasswordLength;
        position += 1;
        Encoding.UTF8.GetBytes(currentPassword, bytes[position..(position + currentPasswordLength)]);
        position += currentPasswordLength;
        bytes[position] = (byte)newPasswordLength;
        position += 1;
        Encoding.UTF8.GetBytes(newPassword, bytes[position..(position + newPasswordLength)]);
        return bytes.ToArray();
    }

    internal static byte[] UpdatePermissions(Identifier userId, Permissions? permissions)
    {
        var length = userId.Length + 2 +
                     (permissions is not null ? 1 + 4 + CalculatePermissionsSize(permissions) : 0);
        Span<byte> bytes = stackalloc byte[length];
        bytes.WriteBytesFromIdentifier(userId);
        var position = userId.Length + 2;
        if (permissions is not null)
        {
            bytes[position++] = 1;
            var permissionsBytes = GetBytesFromPermissions(permissions);
            BinaryPrimitives.WriteInt32LittleEndian(bytes[position..(position + 4)],
                permissionsBytes.Length);
            position += 4;
            permissionsBytes.CopyTo(bytes[position..(position + permissionsBytes.Length)]);
        }
        else
        {
            bytes[position++] = 0;
        }

        return bytes.ToArray();
    }

    internal static byte[] UpdateUser(Identifier userId, string? userName, UserStatus? status)
    {
        var userNameLength = userName is null ? 0 : Encoding.UTF8.GetByteCount(userName);
        var length = userId.Length + 2 + userNameLength
                     + (status is not null ? 2 : 1) + 1 + 1;
        Span<byte> bytes = stackalloc byte[length];

        bytes.WriteBytesFromIdentifier(userId);
        var position = userId.Length + 2;
        if (userName is not null)
        {
            bytes[position] = 1;
            position += 1;
            bytes[position] = (byte)userNameLength;
            position += 1;
            Encoding.UTF8.GetBytes(userName,
                bytes[position..(position + userNameLength)]);
            position += userNameLength;
        }
        else
        {
            bytes[position] = 0;
            position += 1;
        }

        if (status is not null)
        {
            bytes[position++] = 1;
            bytes[position++] = (byte)status;
        }
        else
        {
            bytes[position++] = 0;
        }

        return bytes.ToArray();
    }

    internal static byte[] CreateUser(string userName, string password, UserStatus status,
        Permissions? permissions = null)
    {
        var userNameLength = Encoding.UTF8.GetByteCount(userName);
        var passwordLength = Encoding.UTF8.GetByteCount(password);
        var capacity = 3 + userNameLength + passwordLength
                       + (permissions is not null ? 1 + 4 + CalculatePermissionsSize(permissions) : 1);

        Span<byte> bytes = stackalloc byte[capacity];
        var position = 0;

        bytes[position++] = (byte)userNameLength;
        position += Encoding.UTF8.GetBytes(userName, bytes[position..(position + userNameLength)]);

        bytes[position++] = (byte)passwordLength;
        position += Encoding.UTF8.GetBytes(password, bytes[position..(position + passwordLength)]);

        bytes[position++] = (byte)status;

        if (permissions is not null)
        {
            bytes[position++] = 1;
            var permissionsBytes = GetBytesFromPermissions(permissions);
            BinaryPrimitives.WriteInt32LittleEndian(bytes[position..(position + 4)], permissionsBytes.Length);
            position += 4;
            permissionsBytes.CopyTo(bytes[position..(position + permissionsBytes.Length)]);
        }
        else
        {
            bytes[position++] = 0;
        }

        return bytes.ToArray();
    }

    private static byte[] GetBytesFromPermissions(Permissions data)
    {
        var size = CalculatePermissionsSize(data);
        Span<byte> bytes = stackalloc byte[size];

        bytes[0] = data.Global.ManageServers ? (byte)1 : (byte)0;
        bytes[1] = data.Global.ReadServers ? (byte)1 : (byte)0;
        bytes[2] = data.Global.ManageUsers ? (byte)1 : (byte)0;
        bytes[3] = data.Global.ReadUsers ? (byte)1 : (byte)0;
        bytes[4] = data.Global.ManageStreams ? (byte)1 : (byte)0;
        bytes[5] = data.Global.ReadStreams ? (byte)1 : (byte)0;
        bytes[6] = data.Global.ManageTopics ? (byte)1 : (byte)0;
        bytes[7] = data.Global.ReadTopics ? (byte)1 : (byte)0;
        bytes[8] = data.Global.PollMessages ? (byte)1 : (byte)0;
        bytes[9] = data.Global.SendMessages ? (byte)1 : (byte)0;


        if (data.Streams is not null)
        {
            var streamsCount = data.Streams.Count;
            var currentStream = 1;
            bytes[10] = 1;
            var position = 11;
            foreach (var (streamId, stream) in data.Streams)
            {
                BinaryPrimitives.WriteInt32LittleEndian(bytes[position..(position + 4)], streamId);
                position += 4;

                bytes[position] = stream.ManageStream ? (byte)1 : (byte)0;
                bytes[position + 1] = stream.ReadStream ? (byte)1 : (byte)0;
                bytes[position + 2] = stream.ManageTopics ? (byte)1 : (byte)0;
                bytes[position + 3] = stream.ReadTopics ? (byte)1 : (byte)0;
                bytes[position + 4] = stream.PollMessages ? (byte)1 : (byte)0;
                bytes[position + 5] = stream.SendMessages ? (byte)1 : (byte)0;
                position += 6;

                if (stream.Topics != null)
                {
                    var topicsCount = stream.Topics.Count;
                    var currentTopic = 1;
                    bytes[position] = 1;
                    position += 1;

                    foreach (var (topicId, topic) in stream.Topics)
                    {
                        BinaryPrimitives.WriteInt32LittleEndian(bytes[position..(position + 4)], topicId);
                        position += 4;

                        bytes[position] = topic.ManageTopic ? (byte)1 : (byte)0;
                        bytes[position + 1] = topic.ReadTopic ? (byte)1 : (byte)0;
                        bytes[position + 2] = topic.PollMessages ? (byte)1 : (byte)0;
                        bytes[position + 3] = topic.SendMessages ? (byte)1 : (byte)0;
                        position += 4;
                        if (currentTopic < topicsCount)
                        {
                            currentTopic++;
                            bytes[position++] = 1;
                        }
                        else
                        {
                            bytes[position++] = 0;
                        }
                    }
                }
                else
                {
                    bytes[position++] = 0;
                }

                if (currentStream < streamsCount)
                {
                    currentStream++;
                    bytes[position++] = 1;
                }
                else
                {
                    bytes[position++] = 0;
                }
            }
        }
        else
        {
            bytes[0] = 0;
        }

        return bytes.ToArray();
    }

    private static int CalculatePermissionsSize(Permissions data)
    {
        var size = 10;

        if (data.Streams is not null)
        {
            size += 1;
            foreach (var (_, stream) in data.Streams)
            {
                size += 4;
                size += 6;
                size += 1;

                if (stream.Topics is not null)
                {
                    size += 1;
                    size += stream.Topics.Count * 9;
                }
                else
                {
                    size += 1;
                }
            }
        }
        else
        {
            size += 1;
        }

        return size;
    }

    public static byte[] FlushUnsavedBuffer(Identifier streamId, Identifier topicId, uint partitionId, bool fsync)
    {
        var length = streamId.Length + 2 + topicId.Length + 2 + 4 + 1;
        Span<byte> bytes = stackalloc byte[length];
        bytes.WriteBytesFromStreamAndTopicIdentifiers(streamId, topicId);
        var position = streamId.Length + 2 + topicId.Length + 2;
        BinaryPrimitives.WriteUInt32LittleEndian(bytes[position..(position + 4)], partitionId);
        bytes[position + 4] = fsync ? (byte)1 : (byte)0;

        return bytes.ToArray();
    }

    internal static void GetMessages(Span<byte> bytes, Consumer consumer, Identifier streamId, Identifier topicId,
        PollingStrategy pollingStrategy,
        uint count, bool autoCommit, uint? partitionId)
    {
        bytes[0] = GetConsumerTypeByte(consumer.Type);
        bytes.WriteBytesFromIdentifier(consumer.ConsumerId, 1);
        var position = 1 + consumer.ConsumerId.Length + 2;
        bytes.WriteBytesFromStreamAndTopicIdentifiers(streamId, topicId, position);
        position += 2 + streamId.Length + 2 + topicId.Length;

        // Encode partition_id with a flag byte: 1 = Some, 0 = None
        if (partitionId.HasValue)
        {
            bytes[position] = 1; // Flag byte: partition_id is Some
            BinaryPrimitives.WriteUInt32LittleEndian(bytes[(position + 1)..(position + 5)], partitionId.Value);
        }
        else
        {
            bytes[position] = 0; // Flag byte: partition_id is None
            BinaryPrimitives.WriteUInt32LittleEndian(bytes[(position + 1)..(position + 5)], 0); // Padding
        }

        bytes[position + 5] = GetPollingStrategyByte(pollingStrategy.Kind);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes[(position + 6)..(position + 14)], pollingStrategy.Value);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes[(position + 14)..(position + 18)], count);

        bytes[position + 18] = autoCommit ? (byte)1 : (byte)0;
    }

    /// <summary>
    ///     Encodes a SendMessages body: routing metadata followed by one canonical batch record
    ///     (a 256-byte batch header plus per-message frames). The server stamps
    ///     <c>partition_id</c>, <c>base_offset</c>, and <c>base_timestamp</c>; they stay zero here.
    /// </summary>
    internal static int CreateMessage(Span<byte> bytes, Identifier streamId, Identifier topicId,
        Partitioning partitioning, ReadOnlySpan<Message> messages, IMessageEncryptor? encryptor = null)
    {
        if (messages.IsEmpty)
        {
            throw new ArgumentException("Batch must contain at least one message.", nameof(messages));
        }

        var metadataLength = 2 + streamId.Length + 2 + topicId.Length + 2 + partitioning.Length + 4;
        BinaryPrimitives.WriteInt32LittleEndian(bytes[..4], metadataLength);
        bytes.WriteBytesFromStreamAndTopicIdentifiers(streamId, topicId, 4);
        var position = 2 + streamId.Length + 2 + topicId.Length + 4;
        bytes.WriteBytesFromPartitioning(partitioning, position);
        position += 2 + partitioning.Length;
        BinaryPrimitives.WriteInt32LittleEndian(bytes[position..(position + 4)], messages.Length);
        position += 4;

        // The producer owns message ids: a zero id is minted before the frame checksum covers it.
        var originTimestamp = ulong.MaxValue;
        foreach (var message in messages)
        {
            if (message.Header.Id == 0)
            {
                message.Header = message.Header with { Id = Guid.NewGuid().ToUInt128() };
            }

            originTimestamp = Math.Min(originTimestamp, message.Header.OriginTimestamp);
        }

        var batchStart = position;
        bytes[batchStart..(batchStart + BatchWireFormat.BATCH_HEADER_SIZE)].Clear();
        position += BatchWireFormat.BATCH_HEADER_SIZE;
        var blobStart = position;
        var offsetDelta = 0u;

        // One scratch buffer reused across the batch (grown on demand); holds plaintext headers, so it is
        // returned cleared on every path, including a mid-batch throw from the encryptor or header serialization.
        byte[]? headerScratch = null;
        try
        {
            foreach (var message in messages)
            {
                var header = message.Header;
                var timestampDelta = header.OriginTimestamp - originTimestamp;
                if (timestampDelta > uint.MaxValue)
                {
                    throw new ArgumentException(
                        $"Message origin timestamp runs {timestampDelta} microseconds ahead of the batch's " +
                        $"earliest one; the frame field holds at most {uint.MaxValue}.", nameof(messages));
                }

                var frameStart = position;
                var payloadStart = frameStart + BatchWireFormat.FRAME_HEADER_SIZE;

                int payloadLength;
                if (encryptor is null)
                {
                    payloadLength = message.Payload.Length;
                    message.Payload.Span.CopyTo(bytes[payloadStart..(payloadStart + payloadLength)]);
                }
                else
                {
                    // Bound the destination to this message's reserved share of the buffer so an encryptor that
                    // overruns its GetMaxEncryptedLength contract fails fast here instead of corrupting the batch.
                    Span<byte> payloadDestination =
                        bytes.Slice(payloadStart, encryptor.GetMaxEncryptedLength(message.Payload.Length));
                    payloadLength = encryptor.Encrypt(message.Payload.Span, payloadDestination);
                }

                ReadOnlyMemory<byte> rawHeaders = message.RawUserHeaders;
                var plainHeadersLength =
                    rawHeaders.IsEmpty ? HeadersByteLength(message.UserHeaders) : rawHeaders.Length;
                var headersStart = payloadStart + payloadLength;
                var headersLength = 0;
                if (plainHeadersLength > 0)
                {
                    if (encryptor is null)
                    {
                        headersLength = plainHeadersLength;
                        Span<byte> headersDestination = bytes[headersStart..(headersStart + headersLength)];
                        if (rawHeaders.IsEmpty)
                        {
                            WriteHeadersTo(headersDestination, message.UserHeaders!);
                        }
                        else
                        {
                            rawHeaders.Span.CopyTo(headersDestination);
                        }
                    }
                    else
                    {
                        Span<byte> headersDestination =
                            bytes.Slice(headersStart, encryptor.GetMaxEncryptedLength(plainHeadersLength));
                        if (rawHeaders.IsEmpty)
                        {
                            if (headerScratch is null || headerScratch.Length < plainHeadersLength)
                            {
                                if (headerScratch is not null)
                                {
                                    ArrayPool<byte>.Shared.Return(headerScratch, true);
                                }

                                headerScratch = ArrayPool<byte>.Shared.Rent(plainHeadersLength);
                            }

                            WriteHeadersTo(headerScratch.AsSpan(0, plainHeadersLength), message.UserHeaders!);
                            headersLength = encryptor.Encrypt(headerScratch.AsSpan(0, plainHeadersLength),
                                headersDestination);
                        }
                        else
                        {
                            headersLength = encryptor.Encrypt(rawHeaders.Span, headersDestination);
                        }
                    }
                }

                BinaryPrimitives.WriteUInt128LittleEndian(bytes[(frameStart + 8)..(frameStart + 24)], header.Id);
                BinaryPrimitives.WriteUInt32LittleEndian(bytes[(frameStart + 24)..(frameStart + 28)], offsetDelta);
                BinaryPrimitives.WriteUInt32LittleEndian(bytes[(frameStart + 28)..(frameStart + 32)],
                    (uint)timestampDelta);
                BinaryPrimitives.WriteInt32LittleEndian(bytes[(frameStart + 32)..(frameStart + 36)], headersLength);
                BinaryPrimitives.WriteInt32LittleEndian(bytes[(frameStart + 36)..(frameStart + 40)], payloadLength);
                // Reserved must be zero on the wire; the server rejects non-zero values.
                BinaryPrimitives.WriteUInt64LittleEndian(bytes[(frameStart + 40)..(frameStart + 48)], 0);

                position = headersStart + headersLength;
                var frameChecksum = XxHash3.HashToUInt64(bytes[(frameStart + 8)..position]);
                BinaryPrimitives.WriteUInt64LittleEndian(bytes[frameStart..(frameStart + 8)], frameChecksum);
                offsetDelta++;
            }
        }
        finally
        {
            if (headerScratch is not null)
            {
                ArrayPool<byte>.Shared.Return(headerScratch, true);
            }
        }

        var batchLength = (ulong)(BatchWireFormat.BATCH_HEADER_SIZE + (position - blobStart));
        BinaryPrimitives.WriteUInt64LittleEndian(bytes[(batchStart + 24)..(batchStart + 32)], originTimestamp);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes[(batchStart + 32)..(batchStart + 40)], batchLength);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes[(batchStart + 48)..(batchStart + 52)], (uint)messages.Length);
        var batchChecksum = CalculateBatchChecksum(bytes, batchStart, blobStart, position);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes[(batchStart + 40)..(batchStart + 48)], batchChecksum);

        return position;
    }

    /// <summary>
    ///     Batch checksum: XXH3-64 over the batch header meta fields followed by each frame's stored
    ///     8-byte checksum field in message order. Bodies are bound transitively through the
    ///     per-frame checksums. The header fields must already be backpatched into <paramref name="bytes" />.
    /// </summary>
    private static ulong CalculateBatchChecksum(ReadOnlySpan<byte> bytes, int batchStart, int blobStart, int blobEnd)
    {
        var hasher = new XxHash3();
        hasher.Append(bytes.Slice(batchStart, 40));
        hasher.Append(bytes.Slice(batchStart + 48, 4));
        var cursor = blobStart;
        while (cursor < blobEnd)
        {
            hasher.Append(bytes.Slice(cursor, 8));
            var headersLength = BinaryPrimitives.ReadInt32LittleEndian(bytes[(cursor + 32)..(cursor + 36)]);
            var payloadLength = BinaryPrimitives.ReadInt32LittleEndian(bytes[(cursor + 36)..(cursor + 40)]);
            cursor += BatchWireFormat.FRAME_HEADER_SIZE + payloadLength + headersLength;
        }

        return hasher.GetCurrentHashAsUInt64();
    }

    internal static int HeadersByteLength(Dictionary<HeaderKey, HeaderValue>? headers)
    {
        if (headers is null)
        {
            return 0;
        }

        var length = 0;
        foreach (KeyValuePair<HeaderKey, HeaderValue> kvp in headers)
        {
            length += 1 + 4 + kvp.Key.Value.Length + 1 + 4 + kvp.Value.Value.Length;
        }

        return length;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static byte HeaderKindToByte(HeaderKind kind)
    {
        return kind switch
        {
            HeaderKind.Raw => 1,
            HeaderKind.String => 2,
            HeaderKind.Bool => 3,
            HeaderKind.Int8 => 4,
            HeaderKind.Int16 => 5,
            HeaderKind.Int32 => 6,
            HeaderKind.Int64 => 7,
            HeaderKind.Int128 => 8,
            HeaderKind.Uint8 => 9,
            HeaderKind.Uint16 => 10,
            HeaderKind.Uint32 => 11,
            HeaderKind.Uint64 => 12,
            HeaderKind.Uint128 => 13,
            HeaderKind.Float => 14,
            HeaderKind.Double => 15,
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, null)
        };
    }

    internal static void WriteHeadersTo(Span<byte> destination, Dictionary<HeaderKey, HeaderValue> headers)
    {
        var pos = 0;
        foreach (KeyValuePair<HeaderKey, HeaderValue> kvp in headers)
        {
            destination[pos++] = HeaderKindToByte(kvp.Key.Kind);
            BinaryPrimitives.WriteInt32LittleEndian(destination[pos..(pos + 4)], kvp.Key.Value.Length);
            pos += 4;
            kvp.Key.Value.CopyTo(destination[pos..(pos + kvp.Key.Value.Length)]);
            pos += kvp.Key.Value.Length;

            destination[pos++] = HeaderKindToByte(kvp.Value.Kind);
            BinaryPrimitives.WriteInt32LittleEndian(destination[pos..(pos + 4)], kvp.Value.Value.Length);
            pos += 4;
            kvp.Value.Value.CopyTo(destination[pos..(pos + kvp.Value.Value.Length)]);
            pos += kvp.Value.Value.Length;
        }
    }

    internal static byte[] CreateStream(string name)
    {
        var nameLength = Encoding.UTF8.GetByteCount(name);
        Span<byte> bytes = stackalloc byte[nameLength + 1];
        bytes[0] = (byte)nameLength;
        Encoding.UTF8.GetBytes(name, bytes[1..]);
        return bytes.ToArray();
    }

    internal static byte[] UpdateStream(Identifier streamId, string name)
    {
        var nameLength = Encoding.UTF8.GetByteCount(name);
        Span<byte> bytes = stackalloc byte[streamId.Length + nameLength + 3];
        bytes.WriteBytesFromIdentifier(streamId);
        var position = 2 + streamId.Length;
        bytes[position] = (byte)nameLength;
        Encoding.UTF8.GetBytes(name, bytes[(position + 1)..]);
        return bytes.ToArray();
    }

    internal static byte[] CreateGroup(Identifier streamId, Identifier topicId, string name)
    {
        var nameLength = Encoding.UTF8.GetByteCount(name);
        Span<byte> bytes = stackalloc byte[2 + streamId.Length + 2 + topicId.Length + 1 + nameLength];
        bytes.WriteBytesFromStreamAndTopicIdentifiers(streamId, topicId);
        var position = 2 + streamId.Length + 2 + topicId.Length;
        bytes[position] = (byte)nameLength;
        Encoding.UTF8.GetBytes(name, bytes[(position + 1)..]);
        return bytes.ToArray();
    }

    internal static byte[] JoinGroup(Identifier streamId, Identifier topicId, Identifier groupId)
    {
        Span<byte> bytes = stackalloc byte[2 + streamId.Length + 2 + topicId.Length + groupId.Length + 2];
        bytes.WriteBytesFromStreamAndTopicIdentifiers(streamId, topicId);
        var position = 2 + streamId.Length + 2 + topicId.Length;
        bytes.WriteBytesFromIdentifier(groupId, position);
        return bytes.ToArray();
    }

    internal static byte[] LeaveGroup(Identifier streamId, Identifier topicId, Identifier groupId)
    {
        Span<byte> bytes = stackalloc byte[2 + streamId.Length + 2 + topicId.Length + groupId.Length + 2];
        bytes.WriteBytesFromStreamAndTopicIdentifiers(streamId, topicId);
        var position = 2 + streamId.Length + 2 + topicId.Length;
        bytes.WriteBytesFromIdentifier(groupId, position);
        return bytes.ToArray();
    }

    internal static byte[] DeleteGroup(Identifier streamId, Identifier topicId, Identifier groupId)
    {
        Span<byte> bytes = stackalloc byte[2 + streamId.Length + 2 + topicId.Length + groupId.Length + 2];
        bytes.WriteBytesFromStreamAndTopicIdentifiers(streamId, topicId);
        var position = 2 + streamId.Length + 2 + topicId.Length;
        bytes.WriteBytesFromIdentifier(groupId, position);
        return bytes.ToArray();
    }

    internal static byte[] GetGroups(Identifier streamId, Identifier topicId)
    {
        Span<byte> bytes = stackalloc byte[2 + streamId.Length + 2 + topicId.Length];
        bytes.WriteBytesFromStreamAndTopicIdentifiers(streamId, topicId);
        return bytes.ToArray();
    }

    internal static byte[] GetGroup(Identifier streamId, Identifier topicId, Identifier groupId)
    {
        Span<byte> bytes = stackalloc byte[2 + streamId.Length + 2 + topicId.Length + groupId.Length + 2];
        bytes.WriteBytesFromStreamAndTopicIdentifiers(streamId, topicId);
        var position = 2 + streamId.Length + 2 + topicId.Length;
        bytes.WriteBytesFromIdentifier(groupId, position);
        return bytes.ToArray();
    }

    internal static byte[] UpdateTopic(Identifier streamId, Identifier topicId, string name,
        CompressionAlgorithm compressionAlgorithm, ulong maxTopicSize, ulong messageExpiry,
        IReadOnlyDictionary<string, HeaderValue>? extraOptions = null)
    {
        // Settings ride the options block. A default value means the caller did
        // not set the key, so it is omitted and the server leaves the topic's
        // current value alone.
        var options = new Dictionary<HeaderKey, HeaderValue>();
        // Caller keys first, so a named argument overwrites one of them.
        if (extraOptions is not null)
        {
            foreach (var (key, value) in extraOptions)
            {
                options[HeaderKey.FromString(key)] = value;
            }
        }

        if (compressionAlgorithm != CompressionAlgorithm.None)
        {
            options[HeaderKey.FromString("compression_algorithm")]
                = HeaderValue.FromString(compressionAlgorithm.ToString().ToLowerInvariant());
        }

        if (messageExpiry != 0)
        {
            options[HeaderKey.FromString("message_expiry")] = HeaderValue.FromUInt64(messageExpiry);
        }

        if (maxTopicSize != 0)
        {
            options[HeaderKey.FromString("max_topic_size")] = HeaderValue.FromUInt64(maxTopicSize);
        }

        var optionsLength = HeadersByteLength(options);
        var nameLength = WireNameLength(name, nameof(name));
        var length = 4 + streamId.Length + topicId.Length + 1 + nameLength + optionsLength;
        var rented = length > MaxStackAllocBytes ? ArrayPool<byte>.Shared.Rent(length) : null;
        try
        {
            Span<byte> bytes = rented is null ? stackalloc byte[length] : rented.AsSpan(0, length);
            bytes.WriteBytesFromStreamAndTopicIdentifiers(streamId, topicId);
            var position = 4 + streamId.Length + topicId.Length;
            bytes[position] = (byte)nameLength;
            Encoding.UTF8.GetBytes(name, bytes[(position + 1)..(position + 1 + nameLength)]);
            WriteHeadersTo(bytes[(position + 1 + nameLength)..], options);
            return bytes.ToArray();
        }
        finally
        {
            if (rented is not null)
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }

    internal static byte[] CreateTopic(Identifier streamId, string name, uint partitionCount,
        CompressionAlgorithm compressionAlgorithm, ulong messageExpiry,
        ulong maxTopicSize, IReadOnlyDictionary<string, HeaderValue>? extraOptions = null)
    {
        var options = new Dictionary<HeaderKey, HeaderValue>();
        // Caller keys go in first so a named argument overwrites one of them: the
        // block must not carry a key twice, or the server refuses it whole.
        if (extraOptions is not null)
        {
            foreach (var (key, value) in extraOptions)
            {
                options[HeaderKey.FromString(key)] = value;
            }
        }

        if (compressionAlgorithm != CompressionAlgorithm.None)
        {
            options[HeaderKey.FromString("compression_algorithm")]
                = HeaderValue.FromString(compressionAlgorithm.ToString().ToLowerInvariant());
        }

        if (messageExpiry != 0)
        {
            options[HeaderKey.FromString("message_expiry")] = HeaderValue.FromUInt64(messageExpiry);
        }

        if (maxTopicSize != 0)
        {
            options[HeaderKey.FromString("max_topic_size")] = HeaderValue.FromUInt64(maxTopicSize);
        }

        var optionsLength = HeadersByteLength(options);
        var nameLength = WireNameLength(name, nameof(name));
        var length = 2 + streamId.Length + 4 + 1 + nameLength + optionsLength;
        var rented = length > MaxStackAllocBytes ? ArrayPool<byte>.Shared.Rent(length) : null;
        try
        {
            Span<byte> bytes = rented is null ? stackalloc byte[length] : rented.AsSpan(0, length);
            bytes.WriteBytesFromIdentifier(streamId);
            var position = 2 + streamId.Length;
            BinaryPrimitives.WriteUInt32LittleEndian(bytes[position..(position + 4)], partitionCount);
            position += 4;
            bytes[position] = (byte)nameLength;
            Encoding.UTF8.GetBytes(name, bytes[(position + 1)..(position + 1 + nameLength)]);
            WriteHeadersTo(bytes[(position + 1 + nameLength)..], options);
            return bytes.ToArray();
        }
        finally
        {
            if (rented is not null)
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }

    /// <summary>
    ///     UTF-8 byte count of a length-prefixed wire name, bounded by what its one-byte prefix can carry.
    /// </summary>
    private static int WireNameLength(string name, string parameterName)
    {
        var length = Encoding.UTF8.GetByteCount(name);
        if (length > MaxWireNameLength)
        {
            // Truncating into the prefix would ship a frame the server parses as a shorter
            // name followed by garbage, instead of a request it can reject.
            throw new ArgumentException(
                $"{parameterName} must be at most {MaxWireNameLength} UTF-8 bytes, got {length}.", parameterName);
        }

        return length;
    }

    internal static byte[] GetTopicById(Identifier streamId, Identifier topicId)
    {
        Span<byte> bytes = stackalloc byte[2 + streamId.Length + 2 + topicId.Length];
        bytes.WriteBytesFromStreamAndTopicIdentifiers(streamId, topicId);
        return bytes.ToArray();
    }


    internal static byte[] DeleteTopic(Identifier streamId, Identifier topicId)
    {
        Span<byte> bytes = stackalloc byte[2 + streamId.Length + 2 + topicId.Length];
        bytes.WriteBytesFromStreamAndTopicIdentifiers(streamId, topicId);
        return bytes.ToArray();
    }

    internal static byte[] PurgeTopic(Identifier streamId, Identifier topicId)
    {
        Span<byte> bytes = stackalloc byte[2 + streamId.Length + 2 + topicId.Length];
        bytes.WriteBytesFromStreamAndTopicIdentifiers(streamId, topicId);
        return bytes.ToArray();
    }

    internal static byte[] UpdateOffset(Identifier streamId, Identifier topicId, Consumer consumer, ulong offset,
        uint? partitionId)
    {
        Span<byte> bytes =
            stackalloc byte[2 + streamId.Length + 2 + topicId.Length + 14 + 1 + 2 + consumer.ConsumerId.Length];
        bytes[0] = GetConsumerTypeByte(consumer.Type);
        bytes.WriteBytesFromIdentifier(consumer.ConsumerId, 1);
        var position = 1 + consumer.ConsumerId.Length + 2;
        bytes.WriteBytesFromStreamAndTopicIdentifiers(streamId, topicId, position);
        position += 2 + streamId.Length + 2 + topicId.Length;

        // Encode partition_id with a flag byte: 1 = Some, 0 = None
        if (partitionId.HasValue)
        {
            bytes[position] = 1; // Flag byte: partition_id is Some
            BinaryPrimitives.WriteUInt32LittleEndian(bytes[(position + 1)..(position + 5)], partitionId.Value);
        }
        else
        {
            bytes[position] = 0; // Flag byte: partition_id is None
            BinaryPrimitives.WriteUInt32LittleEndian(bytes[(position + 1)..(position + 5)], 0); // Padding
        }

        BinaryPrimitives.WriteUInt64LittleEndian(bytes[(position + 5)..(position + 13)], offset);
        bytes[position + 13] = AckQuorum;
        return bytes.ToArray();
    }

    internal static byte[] GetOffset(Identifier streamId, Identifier topicId, Consumer consumer, uint? partitionId)
    {
        Span<byte> bytes =
            stackalloc byte[2 + streamId.Length + 2 + topicId.Length + 5 + 1 + 2 + consumer.ConsumerId.Length];
        bytes[0] = GetConsumerTypeByte(consumer.Type);
        bytes.WriteBytesFromIdentifier(consumer.ConsumerId, 1);
        var position = 1 + consumer.ConsumerId.Length + 2;
        bytes.WriteBytesFromStreamAndTopicIdentifiers(streamId, topicId, position);
        position += 2 + streamId.Length + 2 + topicId.Length;

        // Encode partition_id with a flag byte: 1 = Some, 0 = None
        if (partitionId.HasValue)
        {
            bytes[position] = 1; // Flag byte: partition_id is Some
            BinaryPrimitives.WriteUInt32LittleEndian(bytes[(position + 1)..(position + 5)], partitionId.Value);
        }
        else
        {
            bytes[position] = 0; // Flag byte: partition_id is None
            BinaryPrimitives.WriteUInt32LittleEndian(bytes[(position + 1)..(position + 5)], 0); // Padding
        }

        return bytes.ToArray();
    }

    internal static byte[] CreatePartitions(Identifier streamId, Identifier topicId, uint partitionsCount)
    {
        Span<byte> bytes = stackalloc byte[2 + streamId.Length + 2 + topicId.Length + sizeof(int)];
        bytes.WriteBytesFromStreamAndTopicIdentifiers(streamId, topicId);
        var position = 2 + streamId.Length + 2 + topicId.Length;
        BinaryPrimitives.WriteUInt32LittleEndian(bytes[position..(position + 4)], partitionsCount);
        return bytes.ToArray();
    }

    internal static byte[] DeletePartitions(Identifier streamId, Identifier topicId, uint partitionsCount)
    {
        Span<byte> bytes = stackalloc byte[2 + streamId.Length + 2 + topicId.Length + sizeof(int)];
        bytes.WriteBytesFromStreamAndTopicIdentifiers(streamId, topicId);
        var position = 2 + streamId.Length + 2 + topicId.Length;
        BinaryPrimitives.WriteUInt32LittleEndian(bytes[position..(position + 4)], partitionsCount);
        return bytes.ToArray();
    }

    internal static byte[] DeleteSegments(Identifier streamId, Identifier topicId, uint partitionId,
        uint segmentsCount)
    {
        // Binary format: [stream_id_bytes][topic_id_bytes][partition_id: u32 LE][segments_count: u32 LE]
        Span<byte> bytes =
            stackalloc byte[2 + streamId.Length + 2 + topicId.Length + sizeof(int) + sizeof(int)];
        bytes.WriteBytesFromStreamAndTopicIdentifiers(streamId, topicId);
        var position = 2 + streamId.Length + 2 + topicId.Length;
        BinaryPrimitives.WriteUInt32LittleEndian(bytes[position..(position + 4)], partitionId);
        position += 4;
        BinaryPrimitives.WriteUInt32LittleEndian(bytes[position..(position + 4)], segmentsCount);
        return bytes.ToArray();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static byte GetConsumerTypeByte(ConsumerType type)
    {
        return type switch
        {
            ConsumerType.Consumer => 1,
            ConsumerType.ConsumerGroup => 2,
            _ => throw new ArgumentOutOfRangeException()
        };
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static byte GetPollingStrategyByte(MessagePolling pollingStrategy)
    {
        return pollingStrategy switch
        {
            MessagePolling.Offset => 1,
            MessagePolling.Timestamp => 2,
            MessagePolling.First => 3,
            MessagePolling.Last => 4,
            MessagePolling.Next => 5,
            _ => throw new ArgumentOutOfRangeException()
        };
    }

    internal static byte[] GetSnapshot(SnapshotCompression compression, IList<SystemSnapshotType> snapshotTypes)
    {
        // Binary format: [compression_code: u8] [types_count: u8] [type_code_1: u8] [type_code_2: u8] ...
        var length = 1 + 1 + snapshotTypes.Count;
        Span<byte> bytes = stackalloc byte[length];
        bytes[0] = (byte)compression;
        bytes[1] = (byte)snapshotTypes.Count;
        for (var i = 0; i < snapshotTypes.Count; i++)
        {
            bytes[2 + i] = (byte)snapshotTypes[i];
        }

        return bytes.ToArray();
    }

    internal static byte[] DeleteOffset(Identifier streamId, Identifier topicId, Consumer consumer, uint? partitionId)
    {
        Span<byte> bytes =
            stackalloc byte[2 + streamId.Length + 2 + topicId.Length + 6 + 1 + 2 + consumer.ConsumerId.Length];
        bytes[0] = GetConsumerTypeByte(consumer.Type);
        bytes.WriteBytesFromIdentifier(consumer.ConsumerId, 1);
        var position = 1 + consumer.ConsumerId.Length + 2;
        bytes.WriteBytesFromStreamAndTopicIdentifiers(streamId, topicId, position);
        position += 2 + streamId.Length + 2 + topicId.Length;

        // Encode partition_id with a flag byte: 1 = Some, 0 = None
        if (partitionId.HasValue)
        {
            bytes[position] = 1; // Flag byte: partition_id is Some
            BinaryPrimitives.WriteUInt32LittleEndian(bytes[(position + 1)..(position + 5)], partitionId.Value);
        }
        else
        {
            bytes[position] = 0; // Flag byte: partition_id is None
            BinaryPrimitives.WriteUInt32LittleEndian(bytes[(position + 1)..(position + 5)], 0); // Padding
        }

        bytes[position + 5] = AckQuorum;
        return bytes.ToArray();
    }
}
