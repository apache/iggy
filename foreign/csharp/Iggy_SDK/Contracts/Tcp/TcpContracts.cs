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
using System.Runtime.CompilerServices;
using System.Text;
using Apache.Iggy.Contracts.Auth;
using Apache.Iggy.Enums;
using Apache.Iggy.Extensions;
using Apache.Iggy.Headers;
using Apache.Iggy.Kinds;
using Apache.Iggy.Messages;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.Contracts.Tcp;

//TODO - write unit tests for all the user related contracts
internal static class TcpContracts
{
    internal static byte[] LoginWithPersonalAccessToken(string token)
    {
        Span<byte> bytes = stackalloc byte[5 + token.Length];
        bytes[0] = (byte)token.Length;
        Encoding.UTF8.GetBytes(token, bytes[1..(1 + token.Length)]);
        return bytes.ToArray();
    }

    internal static byte[] DeletePersonalRequestToken(string name)
    {
        Span<byte> bytes = stackalloc byte[5 + name.Length];
        bytes[0] = (byte)name.Length;
        Encoding.UTF8.GetBytes(name, bytes[1..(1 + name.Length)]);
        return bytes.ToArray();
    }

    internal static byte[] CreatePersonalAccessToken(string name, ulong? expiry)
    {
        Span<byte> bytes = stackalloc byte[1 + name.Length + 8];
        bytes[0] = (byte)name.Length;
        Encoding.UTF8.GetBytes(name, bytes[1..(1 + name.Length)]);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes[(1 + name.Length)..], expiry ?? 0);
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

        // Username
        var usernameLength = (byte)userName.Length;
        bytes.Add(usernameLength);
        bytes.AddRange(Encoding.UTF8.GetBytes(userName));

        // Password
        var passwordLength = (byte)password.Length;
        bytes.Add(passwordLength);
        bytes.AddRange(Encoding.UTF8.GetBytes(password));

        // Version (opcional)
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

        // Context (opcional)
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
        var length = userId.Length + 2 + currentPassword.Length + newPassword.Length + 2;
        Span<byte> bytes = stackalloc byte[length];

        bytes.WriteBytesFromIdentifier(userId);
        var position = userId.Length + 2;
        bytes[position] = (byte)currentPassword.Length;
        position += 1;
        Encoding.UTF8.GetBytes(currentPassword, bytes[position..(position + currentPassword.Length)]);
        position += currentPassword.Length;
        bytes[position] = (byte)newPassword.Length;
        position += 1;
        Encoding.UTF8.GetBytes(newPassword, bytes[position..(position + newPassword.Length)]);
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
            //CalculatePermissionsSize(request.Permissions));
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
        var length = userId.Length + 2 + (userName?.Length ?? 0)
                     + (status is not null ? 2 : 1) + 1 + 1;
        Span<byte> bytes = stackalloc byte[length];

        bytes.WriteBytesFromIdentifier(userId);
        var position = userId.Length + 2;
        if (userName is not null)
        {
            bytes[position] = 1;
            position += 1;
            bytes[position] = (byte)userName.Length;
            position += 1;
            Encoding.UTF8.GetBytes(userName,
                bytes[position..(position + userName.Length)]);
            position += userName.Length;
        }
        else
        {
            bytes[userId.Length] = 0;
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
        var capacity = 3 + userName.Length + password.Length
                       + (permissions is not null ? 1 + 4 + CalculatePermissionsSize(permissions) : 1);

        Span<byte> bytes = stackalloc byte[capacity];
        var position = 0;

        bytes[position++] = (byte)userName.Length;
        position += Encoding.UTF8.GetBytes(userName, bytes[position..(position + userName.Length)]);

        bytes[position++] = (byte)password.Length;
        position += Encoding.UTF8.GetBytes(password, bytes[position..(position + password.Length)]);

        bytes[position++] = (byte)status;

        if (permissions is not null)
        {
            bytes[position++] = 1;
            var permissionsBytes = GetBytesFromPermissions(permissions);
            //BinaryPrimitives.WriteInt32LittleEndian(bytes[position..(position + 4)], CalculatePermissionsSize(permissions));
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

    internal static void CreateMessage(Span<byte> bytes, Identifier streamId, Identifier topicId,
        Partitioning partitioning, IList<Message> messages)
    {
        var metadataLength = 2 + streamId.Length + 2 + topicId.Length + 2 + partitioning.Length + 4;
        BinaryPrimitives.WriteInt32LittleEndian(bytes[..4], metadataLength);
        bytes.WriteBytesFromStreamAndTopicIdentifiers(streamId, topicId, 4);
        var position = 2 + streamId.Length + 2 + topicId.Length + 4;
        bytes.WriteBytesFromPartitioning(partitioning, position);
        position += 2 + partitioning.Length;
        BinaryPrimitives.WriteInt32LittleEndian(bytes[position..(position + 4)], messages.Count);
        position += 4;

        var indexPosition = position;
        position += 16 * messages.Count;

        var msgSize = 0;
        foreach (var message in messages)
        {
            var headersBytes = GetHeadersBytes(message.UserHeaders);
            BinaryPrimitives.WriteUInt64LittleEndian(bytes[position..(position + 8)], message.Header.Checksum);
            BinaryPrimitives.WriteUInt128LittleEndian(bytes[(position + 8)..(position + 24)], message.Header.Id);
            BinaryPrimitives.WriteUInt64LittleEndian(bytes[(position + 24)..(position + 32)], message.Header.Offset);
            BinaryPrimitives.WriteUInt64LittleEndian(bytes[(position + 32)..(position + 40)],
                DateTimeOffsetUtils.ToUnixTimeMicroSeconds(message.Header.Timestamp));
            BinaryPrimitives.WriteUInt64LittleEndian(bytes[(position + 40)..(position + 48)],
                message.Header.OriginTimestamp);
            BinaryPrimitives.WriteInt32LittleEndian(bytes[(position + 48)..(position + 52)], headersBytes.Length);
            BinaryPrimitives.WriteInt32LittleEndian(bytes[(position + 52)..(position + 56)], message.Payload.Length);

            message.Payload.CopyTo(bytes[(position + 56)..(position + 56 + message.Header.PayloadLength)]);
            if (headersBytes.Length > 0)
            {
                headersBytes
                    .CopyTo(bytes[
                        (position + 56 + message.Header.PayloadLength)..(position + 56 + message.Header.PayloadLength +
                                                                         headersBytes.Length)]);
            }

            position += 56 + message.Header.PayloadLength + headersBytes.Length;

            msgSize += message.GetSize() + headersBytes.Length;

            BinaryPrimitives.WriteInt32LittleEndian(bytes[indexPosition..(indexPosition + 4)], 0);
            BinaryPrimitives.WriteInt32LittleEndian(bytes[(indexPosition + 4)..(indexPosition + 8)], msgSize);
            BinaryPrimitives.WriteInt64LittleEndian(bytes[(indexPosition + 8)..(indexPosition + 16)], 0);
            indexPosition += 16;
        }
    }

    // private static Span<byte> HandleMessagesIList(int position, IList<Message> messages, Span<byte> bytes)
    // {
    //     Span<byte> emptyHeaders = stackalloc byte[4];
    //
    //     foreach (var message in messages)
    //     {
    //         var idSlice = bytes[position..(position + 16)];
    //         //TODO - this required testing on different cpu architectures
    //         Unsafe.WriteUnaligned(ref MemoryMarshal.GetReference(idSlice), message.Id);
    //
    //         if (message.Headers is not null)
    //         {
    //             var headersBytes = GetHeadersBytes(message.Headers);
    //             BinaryPrimitives.WriteInt32LittleEndian(bytes[(position + 16)..(position + 20)], headersBytes.Length);
    //             headersBytes.CopyTo(bytes[(position + 20)..(position + 20 + headersBytes.Length)]);
    //             position += headersBytes.Length + 20;
    //         }
    //         else
    //         {
    //             emptyHeaders.CopyTo(bytes[(position + 16)..(position + 16 + emptyHeaders.Length)]);
    //             position += 20;
    //         }
    //
    //         BinaryPrimitives.WriteInt32LittleEndian(bytes[(position)..(position + 4)], message.Payload.Length);
    //         var payloadBytes = message.Payload;
    //         var slice = bytes[(position + 4)..];
    //         payloadBytes.CopyTo(slice);
    //         position += payloadBytes.Length + 4;
    //     }
    //
    //     return bytes;
    // }
    // private static Span<byte> HandleMessagesArray(int position, Message[] messages, Span<byte> bytes)
    // {
    //     Span<byte> emptyHeaders = stackalloc byte[4];
    //
    //     ref var start = ref MemoryMarshal.GetArrayDataReference(messages);
    //     ref var end = ref Unsafe.Add(ref start, messages.Length);
    //     while (Unsafe.IsAddressLessThan(ref start, ref end))
    //     {
    //         var idSlice = bytes[position..(position + 16)];
    //         //TODO - this required testing on different cpu architectures
    //         Unsafe.WriteUnaligned(ref MemoryMarshal.GetReference(idSlice), start.Id);
    //
    //         if (start.Headers is not null)
    //         {
    //             var headersBytes = GetHeadersBytes(start.Headers);
    //             BinaryPrimitives.WriteInt32LittleEndian(bytes[(position + 16)..(position + 20)], headersBytes.Length);
    //             headersBytes.CopyTo(bytes[(position + 20)..(position + 20 + headersBytes.Length)]);
    //             position += headersBytes.Length + 20;
    //         }
    //         else
    //         {
    //             emptyHeaders.CopyTo(bytes[(position + 16)..(position + 16 + emptyHeaders.Length)]);
    //             position += 20;
    //         }
    //
    //         BinaryPrimitives.WriteInt32LittleEndian(bytes[(position)..(position + 4)], start.Payload.Length);
    //         var payloadBytes = start.Payload;
    //         var slice = bytes[(position + 4)..];
    //         payloadBytes.CopyTo(slice);
    //         position += payloadBytes.Length + 4;
    //
    //         start = ref Unsafe.Add(ref start, 1);
    //     }
    //
    //     return bytes;
    // }
    // private static Span<byte> HandleMessagesList(int position, List<Message> messages, Span<byte> bytes)
    // {
    //     Span<byte> emptyHeaders = stackalloc byte[4];
    //
    //     Span<Message> listAsSpan = CollectionsMarshal.AsSpan(messages);
    //     ref var start = ref MemoryMarshal.GetReference(listAsSpan);
    //     ref var end = ref Unsafe.Add(ref start, listAsSpan.Length);
    //     while (Unsafe.IsAddressLessThan(ref start, ref end))
    //     {
    //         var idSlice = bytes[position..(position + 16)];
    //         //TODO - this required testing on different cpu architectures
    //         Unsafe.WriteUnaligned(ref MemoryMarshal.GetReference(idSlice), start.Id);
    //
    //         if (start.Headers is not null)
    //         {
    //             var headersBytes = GetHeadersBytes(start.Headers);
    //             BinaryPrimitives.WriteInt32LittleEndian(bytes[(position + 16)..(position + 20)], headersBytes.Length);
    //             headersBytes.CopyTo(bytes[(position + 20)..(position + 20 + headersBytes.Length)]);
    //             position += headersBytes.Length + 20;
    //         }
    //         else
    //         {
    //             emptyHeaders.CopyTo(bytes[(position + 16)..(position + 16 + emptyHeaders.Length)]);
    //             position += 20;
    //         }
    //
    //         BinaryPrimitives.WriteInt32LittleEndian(bytes[(position)..(position + 4)], start.Payload.Length);
    //         var payloadBytes = start.Payload;
    //         var slice = bytes[(position + 4)..];
    //         payloadBytes.CopyTo(slice);
    //         position += payloadBytes.Length + 4;
    //
    //         start = ref Unsafe.Add(ref start, 1);
    //     }
    //
    //     return bytes;
    // }

    private static byte[] GetHeadersBytes(Dictionary<HeaderKey, HeaderValue>? headers)
    {
        if (headers == null)
        {
            return [];
        }

        var headersLength = headers.Sum(header => 4 + header.Key.Value.Length + 1 + 4 + header.Value.Value.Length);
        Span<byte> headersBytes = stackalloc byte[headersLength];
        var position = 0;
        foreach (var (headerKey, headerValue) in headers)
        {
            var headerBytes = GetBytesFromHeader(headerKey, headerValue);
            headerBytes.CopyTo(headersBytes[position..(position + headerBytes.Length)]);
            position += headerBytes.Length;
        }

        return headersBytes.ToArray();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static byte HeaderKindToByte(HeaderKind kind)
    {
        return kind switch
        {
            HeaderKind.Raw => 1,
            HeaderKind.String => 2,
            HeaderKind.Bool => 3,
            HeaderKind.Int32 => 6,
            HeaderKind.Int64 => 7,
            HeaderKind.Int128 => 8,
            HeaderKind.Uint32 => 11,
            HeaderKind.Uint64 => 12,
            HeaderKind.Uint128 => 13,
            HeaderKind.Float => 14,
            HeaderKind.Double => 15,
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, null)
        };
    }

    private static byte[] GetBytesFromHeader(HeaderKey headerKey, HeaderValue headerValue)
    {
        var headerBytesLength = 4 + headerKey.Value.Length + 1 + 4 + headerValue.Value.Length;
        Span<byte> headerBytes = stackalloc byte[headerBytesLength];

        BinaryPrimitives.WriteInt32LittleEndian(headerBytes[..4], headerKey.Value.Length);
        var headerKeyBytes = Encoding.UTF8.GetBytes(headerKey.Value);
        headerKeyBytes.CopyTo(headerBytes[4..(4 + headerKey.Value.Length)]);

        headerBytes[4 + headerKey.Value.Length] = HeaderKindToByte(headerValue.Kind);

        BinaryPrimitives.WriteInt32LittleEndian(
            headerBytes[(4 + headerKey.Value.Length + 1)..(4 + headerKey.Value.Length + 1 + 4)],
            headerValue.Value.Length);
        headerValue.Value.CopyTo(headerBytes[(4 + headerKey.Value.Length + 1 + 4)..]);

        return headerBytes.ToArray();
    }

    internal static byte[] CreateStream(string name)
    {
        Span<byte> bytes = stackalloc byte[name.Length + 1];
        bytes[0] = (byte)name.Length;
        Encoding.UTF8.GetBytes(name, bytes[1..]);
        return bytes.ToArray();
    }

    internal static byte[] UpdateStream(Identifier streamId, string name)
    {
        Span<byte> bytes = stackalloc byte[streamId.Length + name.Length + 3];
        bytes.WriteBytesFromIdentifier(streamId);
        var position = 2 + streamId.Length;
        bytes[position] = (byte)name.Length;
        Encoding.UTF8.GetBytes(name, bytes[(position + 1)..]);
        return bytes.ToArray();
    }

    internal static byte[] CreateGroup(Identifier streamId, Identifier topicId, string name)
    {
        Span<byte> bytes = stackalloc byte[2 + streamId.Length + 2 + topicId.Length + 1 + name.Length];
        bytes.WriteBytesFromStreamAndTopicIdentifiers(streamId, topicId);
        var position = 2 + streamId.Length + 2 + topicId.Length;
        bytes[position] = (byte)name.Length;
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
        CompressionAlgorithm compressionAlgorithm, ulong maxTopicSize, ulong messageExpiry, byte? replicationFactor)
    {
        Span<byte> bytes = stackalloc byte[4 + streamId.Length + topicId.Length + 19 + name.Length];
        bytes.WriteBytesFromStreamAndTopicIdentifiers(streamId, topicId);
        var position = 4 + streamId.Length + topicId.Length;
        bytes[position] = (byte)compressionAlgorithm;
        position += 1;
        BinaryPrimitives.WriteUInt64LittleEndian(bytes[position..(position + 8)],
            messageExpiry);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes[(position + 8)..(position + 16)],
            maxTopicSize);
        bytes[position + 16] = replicationFactor ?? 0;
        bytes[position + 17] = (byte)name.Length;
        Encoding.UTF8.GetBytes(name, bytes[(position + 18)..]);
        return bytes.ToArray();
    }

    internal static byte[] CreateTopic(Identifier streamId, string name, uint partitionCount,
        CompressionAlgorithm compressionAlgorithm, byte? replicationFactor, ulong messageExpiry,
        ulong maxTopicSize)
    {
        Span<byte> bytes = stackalloc byte[2 + streamId.Length + 23 + name.Length];
        bytes.WriteBytesFromIdentifier(streamId);
        var position = 2 + streamId.Length;
        BinaryPrimitives.WriteUInt32LittleEndian(bytes[position..(position + 4)], partitionCount);
        bytes[position + 4] = (byte)compressionAlgorithm;
        BinaryPrimitives.WriteUInt64LittleEndian(bytes[(position + 5)..(position + 13)], messageExpiry);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes[(position + 13)..(position + 21)], maxTopicSize);
        bytes[position + 21] = replicationFactor ?? 0;
        bytes[position + 22] = (byte)name.Length;
        Encoding.UTF8.GetBytes(name, bytes[(position + 23)..]);
        return bytes.ToArray();
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
            stackalloc byte[2 + streamId.Length + 2 + topicId.Length + 13 + 1 + 2 + consumer.ConsumerId.Length];
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

    internal static byte[] DeleteOffset(Identifier streamId, Identifier topicId, Consumer consumer, uint? partitionId)
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
}
