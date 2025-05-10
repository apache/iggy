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

using Iggy_SDK.Contracts.Http;
using Iggy_SDK.Contracts.Http.Auth;
using Iggy_SDK.Enums;
using Iggy_SDK.Extensions;
using Iggy_SDK.Headers;
using Iggy_SDK.Messages;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using Partitioning = Iggy_SDK.Enums.Partitioning;

namespace Iggy_SDK.Contracts.Tcp;

//TODO - write unit tests for all the user related contracts
internal static class TcpContracts
{
    internal static byte[] LoginWithPersonalAccessToken(LoginWithPersonalAccessToken request)
    {
        Span<byte> bytes = stackalloc byte[5 + request.Token.Length];
        bytes[0] = (byte)request.Token.Length;
        Encoding.UTF8.GetBytes(request.Token, bytes[1..(1 + request.Token.Length)]);
        return bytes.ToArray();
    }
    internal static byte[] DeletePersonalRequestToken(DeletePersonalAccessTokenRequest request)
    {
        Span<byte> bytes = stackalloc byte[5 + request.Name.Length];
        bytes[0] = (byte)request.Name.Length;
        Encoding.UTF8.GetBytes(request.Name, bytes[1..(1 + request.Name.Length)]);
        return bytes.ToArray();
    }
    internal static byte[] CreatePersonalAccessToken(CreatePersonalAccessTokenRequest request)
    {
        Span<byte> bytes = stackalloc byte[5 + request.Name.Length];
        bytes[0] = (byte)request.Name.Length;
        Encoding.UTF8.GetBytes(request.Name, bytes[1..(1 + request.Name.Length)]);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes[(1 + request.Name.Length)..], request.Expiry ?? 0);
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
    internal static byte[] LoginUser(LoginUserRequest request)
    {
        List<byte> bytes = new List<byte>();

        // Username
        byte usernameLength = (byte)request.Username.Length;
        bytes.Add(usernameLength);
        bytes.AddRange(Encoding.UTF8.GetBytes(request.Username));

        // Password
        byte passwordLength = (byte)request.Password.Length;
        bytes.Add(passwordLength);
        bytes.AddRange(Encoding.UTF8.GetBytes(request.Password));

        // Version (opcional)
        if (!string.IsNullOrEmpty(request.Version))
        {
            byte[] versionBytes = Encoding.UTF8.GetBytes(request.Version);
            bytes.AddRange(BitConverter.GetBytes(versionBytes.Length)); // tamanho da versão (u32, little-endian)
            bytes.AddRange(versionBytes);
        }
        else
        {
            bytes.AddRange(BitConverter.GetBytes(0)); // tamanho 0 para versão ausente
        }

        // Context (opcional)
        if (!string.IsNullOrEmpty(request.Context))
        {
            byte[] contextBytes = Encoding.UTF8.GetBytes(request.Context);
            bytes.AddRange(BitConverter.GetBytes(contextBytes.Length)); // tamanho do contexto (u32, little-endian)
            bytes.AddRange(contextBytes);
        }
        else
        {
            bytes.AddRange(BitConverter.GetBytes(0)); // tamanho 0 para contexto ausente
        }

        return bytes.ToArray();

        // var length = request.Username.Length + request.Password.Length + 2;
        // Span<byte> bytes = stackalloc byte[length];
        //
        // int position = 0;
        // bytes[position] = (byte)request.Username.Length;
        // position += 1;
        // Encoding.UTF8.GetBytes(request.Username, bytes[position..(position + request.Username.Length)]);
        // position += request.Username.Length;
        // bytes[position] = (byte)request.Password.Length;
        // position += 1;
        // Encoding.UTF8.GetBytes(request.Password, bytes[position..(position + request.Password.Length)]);
        // position += request.Password.Length;

        // return bytes.ToArray();
        //return JsonSerializer.SerializeToUtf8Bytes(request);
    }
    internal static byte[] ChangePassword(ChangePasswordRequest request)
    {
        var length = request.UserId.Length + 2 + request.CurrentPassword.Length + request.NewPassword.Length + 2;
        Span<byte> bytes = stackalloc byte[length];

        bytes.WriteBytesFromIdentifier(request.UserId);
        int position = request.UserId.Length + 2;
        bytes[position] = (byte)request.CurrentPassword.Length;
        position += 1;
        Encoding.UTF8.GetBytes(request.CurrentPassword, bytes[position..(position + request.CurrentPassword.Length)]);
        position += request.CurrentPassword.Length;
        bytes[position] = (byte)request.NewPassword.Length;
        position += 1;
        Encoding.UTF8.GetBytes(request.NewPassword, bytes[position..(position + request.NewPassword.Length)]);
        position += request.NewPassword.Length;
        return bytes.ToArray();
    }
    internal static byte[] UpdatePermissions(UpdateUserPermissionsRequest request)
    {
        var length = request.UserId.Length + 2 +
                     (request.Permissions is not null ? 1 + 4 + CalculatePermissionsSize(request.Permissions) : 0);
        Span<byte> bytes = stackalloc byte[length];
        bytes.WriteBytesFromIdentifier(request.UserId);
        int position = request.UserId.Length + 2;
        if (request.Permissions is not null)
        {
            bytes[position++] = 1;
            var permissions = GetBytesFromPermissions(request.Permissions);
            BinaryPrimitives.WriteInt32LittleEndian(bytes[position..(position + 4)],
                CalculatePermissionsSize(request.Permissions));
            position += 4;
            permissions.CopyTo(bytes[position..(position + permissions.Length)]);
            position += permissions.Length;
        }
        else
        {
            bytes[position++] = 0;
        }
        return bytes.ToArray();
    }
    internal static byte[] UpdateUser(UpdateUserRequest request)
    {
        var length = request.UserId.Length + 2 + (request.Username?.Length ?? 0)
                     + (request.UserStatus is not null ? 2 : 1) + 1 + 1;
        Span<byte> bytes = stackalloc byte[length];

        bytes.WriteBytesFromIdentifier(request.UserId);
        int position = request.UserId.Length + 2;
        if (request.Username is not null)
        {
            bytes[position] = 1;
            position += 1;
            bytes[position] = (byte)request.Username.Length;
            position += 1;
            Encoding.UTF8.GetBytes(request.Username,
                bytes[(position)..(position + request.Username.Length)]);
            position += request.Username.Length;
        }
        else
        {
           bytes[request.UserId.Length] = 0;
           position += 1;
        }

        if (request.UserStatus is not null)
        {
            bytes[position++] = 1;
            bytes[position++] = request.UserStatus switch
            {
                UserStatus.Active => 1,
                UserStatus.Inactive => 2,
                _ => throw new ArgumentOutOfRangeException()
            };
        }
        else
        {
            bytes[position++] = 0;
        }
        return bytes.ToArray();
    }
    internal static byte[] CreateUser(CreateUserRequest request)
    {
        int capacity = 3 + request.Username.Length + request.Password.Length
            + (request.Permissions is not null ? 1 + 4 + CalculatePermissionsSize(request.Permissions) : 0);

        Span<byte> bytes = stackalloc byte[capacity];
        int position = 0;

        bytes[position++] = (byte)request.Username.Length;
        position += Encoding.UTF8.GetBytes(request.Username, bytes[position..(position + request.Username.Length)]);

        bytes[position++] = (byte)request.Password.Length;
        position += Encoding.UTF8.GetBytes(request.Password, bytes[position..(position + request.Password.Length)]);

        bytes[position++] = request.Status switch
        {
            UserStatus.Active => (byte)1,
            UserStatus.Inactive => (byte)2,
            _ => throw new ArgumentOutOfRangeException()
        };

        if (request.Permissions is not null)
        {
            bytes[position++] = 1;
            var permissions = GetBytesFromPermissions(request.Permissions);
            BinaryPrimitives.WriteInt32LittleEndian(bytes[position..(position + 4)], CalculatePermissionsSize(request.Permissions));
            position += 4;
            permissions.CopyTo(bytes[position..(position + permissions.Length)]);
            position += permissions.Length;
        }
        else
        {
            bytes[position++] = 0;
        }

        return bytes.ToArray();
    }
    private static byte[] GetBytesFromPermissions(Permissions data)
    {
        int size = CalculatePermissionsSize(data);
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
            int streamsCount = data.Streams.Count;
            int currentStream = 1;
            bytes[10] = (byte)1;
            int position = 11;
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
                    int topicsCount = stream.Topics.Count;
                    int currentTopic = 1;
                    bytes[position] = (byte)1;
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
                            bytes[position++] = (byte)1;
                        }
                        else
                        {
                            bytes[position++] = (byte)0;
                        }
                    }
                }
                else
                {
                    bytes[position++] = (byte)0;
                }
                if (currentStream < streamsCount)
                {
                    currentStream++;
                    bytes[position++] = (byte)1;
                }
                else
                {
                    bytes[position++] = (byte)0;
                }
            }
        }
        else
        {
            bytes[0] = (byte)0;
        }

        return bytes.ToArray();
    }
    private static int CalculatePermissionsSize(Permissions data)
    {
        int size = 10;

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
    internal static void GetMessages(Span<byte> bytes, MessageFetchRequest request)
    {
        bytes[0] = GetConsumerTypeByte(request.Consumer.Type);
        bytes.WriteBytesFromIdentifier(request.Consumer.Id, 1);
        var position = 1 + request.Consumer.Id.Length + 2;
        bytes.WriteBytesFromStreamAndTopicIdentifiers(request.StreamId, request.TopicId, position);
        position += 2 + request.StreamId.Length + 2 + request.TopicId.Length;
        BinaryPrimitives.WriteInt32LittleEndian(bytes[position..(position + 4)], request.PartitionId);
        bytes[position + 4] = GetPollingStrategyByte(request.PollingStrategy.Kind);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes[(position + 5)..(position + 13)], request.PollingStrategy.Value);
        BinaryPrimitives.WriteInt32LittleEndian(bytes[(position + 13)..(position + 17)], request.Count);

        bytes[position + 17] = request.AutoCommit ? (byte)1 : (byte)0;
    }
    internal static void CreateMessage(Span<byte> bytes, Identifier streamId, Identifier topicId,
        Kinds.Partitioning partitioning, IList<Message> messages)
    {
        bytes.WriteBytesFromStreamAndTopicIdentifiers(streamId, topicId);
        int streamTopicIdPosition = 2 + streamId.Length + 2 + topicId.Length;
        bytes[streamTopicIdPosition] = GetPartitioningKindByte(partitioning.Kind);
        bytes[streamTopicIdPosition + 1] = (byte)partitioning.Length;
        partitioning.Value.CopyTo(bytes[(streamTopicIdPosition + 2)..(streamTopicIdPosition + partitioning.Length + 2)]);

        var position = 2 + partitioning.Length + streamTopicIdPosition;
        bytes = messages switch
        {
            Message[] messagesArray => HandleMessagesArray(position, messagesArray, bytes),
            List<Message> messagesList => HandleMessagesList(position, messagesList, bytes),
            _ => HandleMessagesIList(position, messages, bytes),
        };
    }
    private static Span<byte> HandleMessagesIList(int position, IList<Message> messages, Span<byte> bytes)
    {
        Span<byte> emptyHeaders = stackalloc byte[4];

        foreach (var message in messages)
        {
            var idSlice = bytes[position..(position + 16)];
            //TODO - this required testing on different cpu architectures
            Unsafe.WriteUnaligned(ref MemoryMarshal.GetReference(idSlice), message.Id);

            if (message.Headers is not null)
            {
                var headersBytes = GetHeadersBytes(message.Headers);
                BinaryPrimitives.WriteInt32LittleEndian(bytes[(position + 16)..(position + 20)], headersBytes.Length);
                headersBytes.CopyTo(bytes[(position + 20)..(position + 20 + headersBytes.Length)]);
                position += headersBytes.Length + 20;
            }
            else
            {
                emptyHeaders.CopyTo(bytes[(position + 16)..(position + 16 + emptyHeaders.Length)]);
                position += 20;
            }

            BinaryPrimitives.WriteInt32LittleEndian(bytes[(position)..(position + 4)], message.Payload.Length);
            var payloadBytes = message.Payload;
            var slice = bytes[(position + 4)..];
            payloadBytes.CopyTo(slice);
            position += payloadBytes.Length + 4;
        }

        return bytes;
    }
    private static Span<byte> HandleMessagesArray(int position, Message[] messages, Span<byte> bytes)
    {
        Span<byte> emptyHeaders = stackalloc byte[4];

        ref var start = ref MemoryMarshal.GetArrayDataReference(messages);
        ref var end = ref Unsafe.Add(ref start, messages.Length);
        while (Unsafe.IsAddressLessThan(ref start, ref end))
        {
            var idSlice = bytes[position..(position + 16)];
            //TODO - this required testing on different cpu architectures
            Unsafe.WriteUnaligned(ref MemoryMarshal.GetReference(idSlice), start.Id);

            if (start.Headers is not null)
            {
                var headersBytes = GetHeadersBytes(start.Headers);
                BinaryPrimitives.WriteInt32LittleEndian(bytes[(position + 16)..(position + 20)], headersBytes.Length);
                headersBytes.CopyTo(bytes[(position + 20)..(position + 20 + headersBytes.Length)]);
                position += headersBytes.Length + 20;
            }
            else
            {
                emptyHeaders.CopyTo(bytes[(position + 16)..(position + 16 + emptyHeaders.Length)]);
                position += 20;
            }

            BinaryPrimitives.WriteInt32LittleEndian(bytes[(position)..(position + 4)], start.Payload.Length);
            var payloadBytes = start.Payload;
            var slice = bytes[(position + 4)..];
            payloadBytes.CopyTo(slice);
            position += payloadBytes.Length + 4;

            start = ref Unsafe.Add(ref start, 1);
        }

        return bytes;
    }
    private static Span<byte> HandleMessagesList(int position, List<Message> messages, Span<byte> bytes)
    {
        Span<byte> emptyHeaders = stackalloc byte[4];

        Span<Message> listAsSpan = CollectionsMarshal.AsSpan(messages);
        ref var start = ref MemoryMarshal.GetReference(listAsSpan);
        ref var end = ref Unsafe.Add(ref start, listAsSpan.Length);
        while (Unsafe.IsAddressLessThan(ref start, ref end))
        {
            var idSlice = bytes[position..(position + 16)];
            //TODO - this required testing on different cpu architectures
            Unsafe.WriteUnaligned(ref MemoryMarshal.GetReference(idSlice), start.Id);

            if (start.Headers is not null)
            {
                var headersBytes = GetHeadersBytes(start.Headers);
                BinaryPrimitives.WriteInt32LittleEndian(bytes[(position + 16)..(position + 20)], headersBytes.Length);
                headersBytes.CopyTo(bytes[(position + 20)..(position + 20 + headersBytes.Length)]);
                position += headersBytes.Length + 20;
            }
            else
            {
                emptyHeaders.CopyTo(bytes[(position + 16)..(position + 16 + emptyHeaders.Length)]);
                position += 20;
            }

            BinaryPrimitives.WriteInt32LittleEndian(bytes[(position)..(position + 4)], start.Payload.Length);
            var payloadBytes = start.Payload;
            var slice = bytes[(position + 4)..];
            payloadBytes.CopyTo(slice);
            position += payloadBytes.Length + 4;

            start = ref Unsafe.Add(ref start, 1);
        }

        return bytes;
    }
    private static byte[] GetHeadersBytes(Dictionary<HeaderKey, HeaderValue> headers)
    {
        var headersLength = headers.Sum(header => 4 + header.Key.Value.Length + 1 + 4 + header.Value.Value.Length);
        Span<byte> headersBytes = stackalloc byte[headersLength];
        int position = 0;
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
    internal static byte[] CreateStream(StreamRequest request)
    {
        Span<byte> bytes = stackalloc byte[4 + request.Name.Length + 1];
        BinaryPrimitives.WriteInt32LittleEndian(bytes[..4], request.StreamId ?? 0);
        bytes[4] = (byte)request.Name.Length;
        Encoding.UTF8.GetBytes(request.Name, bytes[5..]);
        return bytes.ToArray();
    }
    internal static byte[] UpdateStream(Identifier streamId, UpdateStreamRequest request)
    {
        Span<byte> bytes = stackalloc byte[streamId.Length + request.Name.Length + 3];
        bytes.WriteBytesFromIdentifier(streamId);
        int position = 2 + streamId.Length;
        bytes[position] = (byte)request.Name.Length;
        Encoding.UTF8.GetBytes(request.Name, bytes[(position + 1)..]);
        return bytes.ToArray();
    }

    internal static byte[] CreateGroup(CreateConsumerGroupRequest request)
    {
        Span<byte> bytes = stackalloc byte[2 + request.StreamId.Length + 2 + request.TopicId.Length + 4 + 1 + request.Name.Length];
        bytes.WriteBytesFromStreamAndTopicIdentifiers(request.StreamId , request.TopicId);
        int position = 2 + request.StreamId.Length + 2 + request.TopicId.Length;
        BinaryPrimitives.WriteInt32LittleEndian(bytes[position..(position + 4)], request.ConsumerGroupId);
        position += 4;
        bytes[position] = (byte)request.Name.Length;
        Encoding.UTF8.GetBytes(request.Name, bytes[(position + 1)..]);
        return bytes.ToArray();
    }

    internal static byte[] JoinGroup(JoinConsumerGroupRequest request)
    {
        Span<byte> bytes = stackalloc byte[2 + request.StreamId.Length + 2 + request.TopicId.Length + request.ConsumerGroupId.Length + 2];
        bytes.WriteBytesFromStreamAndTopicIdentifiers(request.StreamId , request.TopicId);
        int position = 2 + request.StreamId.Length + 2 + request.TopicId.Length;
        bytes.WriteBytesFromIdentifier(request.ConsumerGroupId, position);
        return bytes.ToArray();
    }
    internal static byte[] LeaveGroup(LeaveConsumerGroupRequest request)
    {
        Span<byte> bytes = stackalloc byte[2 + request.StreamId.Length + 2 + request.TopicId.Length + request.ConsumerGroupId.Length + 2];
        bytes.WriteBytesFromStreamAndTopicIdentifiers(request.StreamId , request.TopicId);
        int position = 2 + request.StreamId.Length + 2 + request.TopicId.Length;
        bytes.WriteBytesFromIdentifier(request.ConsumerGroupId, position);
        return bytes.ToArray();
    }
    internal static byte[] DeleteGroup(Identifier streamId, Identifier topicId, Identifier groupId)
    {
        Span<byte> bytes = stackalloc byte[2 + streamId.Length + 2 + topicId.Length + groupId.Length + 2];
        bytes.WriteBytesFromStreamAndTopicIdentifiers(streamId , topicId);
        int position = 2 + streamId.Length + 2 + topicId.Length;
        bytes.WriteBytesFromIdentifier(groupId, position);
        return bytes.ToArray();
    }

    internal static byte[] GetGroups(Identifier streamId, Identifier topicId)
    {
        Span<byte> bytes = stackalloc byte[2 + streamId.Length + 2 + topicId.Length];
        bytes.WriteBytesFromStreamAndTopicIdentifiers(streamId , topicId);
        return bytes.ToArray();
    }

    internal static byte[] GetGroup(Identifier streamId, Identifier topicId, Identifier groupId)
    {
        Span<byte> bytes = stackalloc byte[2 + streamId.Length + 2 + topicId.Length + groupId.Length + 2];
        bytes.WriteBytesFromStreamAndTopicIdentifiers(streamId , topicId);
        int position = 2 + streamId.Length + 2 + topicId.Length;
        bytes.WriteBytesFromIdentifier(groupId, position);
        return bytes.ToArray();
    }

    internal static byte[] UpdateTopic(Identifier streamId, Identifier topicId, UpdateTopicRequest request)
    {
        Span<byte> bytes = stackalloc byte[streamId.Length + topicId.Length + 18 + request.Name.Length];
        bytes.WriteBytesFromStreamAndTopicIdentifiers(streamId , topicId);
        var position = 4 + streamId.Length + topicId.Length;
        BinaryPrimitives.WriteInt32LittleEndian(bytes[position..(position + 4)],
            request.MessageExpiry);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes[(position + 4)..(position + 12)],
            request.MaxTopicSize);
        bytes[position + 12] = request.ReplicationFactor;
        bytes[position + 13] = (byte)request.Name.Length;
        Encoding.UTF8.GetBytes(request.Name, bytes[(position + 14)..]);
        return bytes.ToArray();
    }

    internal static byte[] CreateTopic(Identifier streamId, TopicRequest request)
    {
        Span<byte> bytes = stackalloc byte[2 + streamId.Length + 22 + request.Name.Length];
        bytes.WriteBytesFromIdentifier(streamId);
        var streamIdBytesLength = 2 + streamId.Length;
        BinaryPrimitives.WriteInt32LittleEndian(bytes[streamIdBytesLength..(streamIdBytesLength + 4)], request.TopicId ?? 0);
        int position = 4 + streamIdBytesLength;
        BinaryPrimitives.WriteInt32LittleEndian(bytes[position..(position + 4)], request.PartitionsCount);
        BinaryPrimitives.WriteInt32LittleEndian(bytes[(position + 4)..(position + 8)], request.MessageExpiry);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes[(position + 8)..(position + 16)], request.MaxTopicSize);
        bytes[position + 16] = request.ReplicationFactor;
        bytes[position + 17] = (byte)request.Name.Length;
        Encoding.UTF8.GetBytes(request.Name, bytes[(position + 18)..]);
        return bytes.ToArray();
    }

    internal static byte[] GetTopicById(Identifier streamId, Identifier topicId)
    {
        Span<byte> bytes = stackalloc byte[2 + streamId.Length + 2 + topicId.Length];
        bytes.WriteBytesFromStreamAndTopicIdentifiers(streamId , topicId);
        return bytes.ToArray();
    }


    internal static byte[] DeleteTopic(Identifier streamId, Identifier topicId)
    {
        Span<byte> bytes = stackalloc byte[2 + streamId.Length + 2 + topicId.Length];
        bytes.WriteBytesFromStreamAndTopicIdentifiers(streamId , topicId);
        return bytes.ToArray();
    }

    internal static byte[] UpdateOffset(StoreOffsetRequest request)
    {
        Span<byte> bytes =
            stackalloc byte[2 + request.StreamId.Length + 2 + request.TopicId.Length + 15 + request.Consumer.Id.Length];
        bytes[0] = GetConsumerTypeByte(request.Consumer.Type);
        bytes.WriteBytesFromIdentifier(request.Consumer.Id, 1);
        var position = 1 + request.Consumer.Id.Length + 2;
        bytes.WriteBytesFromStreamAndTopicIdentifiers(request.StreamId , request.TopicId, position);
        position += 2 + request.StreamId.Length + 2 + request.TopicId.Length;
        BinaryPrimitives.WriteInt32LittleEndian(bytes[position..(position + 4)], request.PartitionId);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes[(position + 4)..(position + 12)], request.Offset);
        return bytes.ToArray();
    }

    internal static byte[] GetOffset(OffsetRequest request)
    {
        Span<byte> bytes =
            stackalloc byte[2 + request.StreamId.Length + 2 + request.TopicId.Length + sizeof(int) * 1 + 1 + 2 + request.Consumer.Id.Length];
        bytes[0] = GetConsumerTypeByte(request.Consumer.Type);
        bytes.WriteBytesFromIdentifier(request.Consumer.Id, 1);
        var position = 1 + request.Consumer.Id.Length + 2;
        bytes.WriteBytesFromStreamAndTopicIdentifiers(request.StreamId , request.TopicId, position);
        position = 7 + 2 + request.StreamId.Length + 2 + request.TopicId.Length;
        BinaryPrimitives.WriteInt32LittleEndian(bytes[position..(position + 4)], request.PartitionId);
        return bytes.ToArray();
    }

    internal static byte[] CreatePartitions(CreatePartitionsRequest request)
    {
        Span<byte> bytes = stackalloc byte[2 + request.StreamId.Length + 2 + request.TopicId.Length + sizeof(int)];
        bytes.WriteBytesFromStreamAndTopicIdentifiers(request.StreamId , request.TopicId);
        int position = 2 + request.StreamId.Length + 2 + request.TopicId.Length;
        BinaryPrimitives.WriteInt32LittleEndian(bytes[position..(position + 4)], request.PartitionsCount);
        return bytes.ToArray();
    }

    internal static byte[] DeletePartitions(DeletePartitionsRequest request)
    {
        Span<byte> bytes = stackalloc byte[2 + request.StreamId.Length + 2 + request.TopicId.Length + sizeof(int)];
        bytes.WriteBytesFromStreamAndTopicIdentifiers(request.StreamId , request.TopicId);
        int position = 2 + request.StreamId.Length + 2 + request.TopicId.Length;
        BinaryPrimitives.WriteInt32LittleEndian(bytes[position..(position + 4)], request.PartitionsCount);
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
    private static byte GetPartitioningKindByte(Partitioning kind)
    {
        return kind switch
        {
            Partitioning.Balanced => 1,
            Partitioning.PartitionId => 2,
            Partitioning.MessageKey => 3,
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
}
