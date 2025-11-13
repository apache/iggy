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
using System.Text;
using Apache.Iggy.ConnectionStream;
using Apache.Iggy.Contracts;
using Apache.Iggy.Contracts.Auth;
using Apache.Iggy.Contracts.Tcp;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Kinds;
using Apache.Iggy.Mappers;
using Apache.Iggy.Messages;
using Apache.Iggy.Utils;
using Microsoft.Extensions.Logging;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.IggyClient.Implementations;

public sealed class TcpMessageStream : IIggyClient, IDisposable
{
    private readonly ILogger<TcpMessageStream> _logger;
    private readonly SemaphoreSlim _semaphore;
    private readonly IConnectionStream _stream;

    internal TcpMessageStream(IConnectionStream stream, ILoggerFactory loggerFactory)
    {
        _stream = stream;
        _logger = loggerFactory.CreateLogger<TcpMessageStream>();
        _semaphore = new SemaphoreSlim(1, 1);
    }

    public void Dispose()
    {
        _stream.Close();
        _stream.Dispose();
        _semaphore.Dispose();
    }

    public async Task<StreamResponse?> CreateStreamAsync(string name, CancellationToken token = default)
    {
        var message = TcpContracts.CreateStream(name);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.CREATE_STREAM_CODE);

        var responseBuffer = await SendWithResponseAsync(payload, token);

        if (responseBuffer.Length == 0)
        {
            throw new InvalidResponseException("Received empty response while trying to create stream.");
        }

        return BinaryMapper.MapStream(responseBuffer);
    }

    public async Task<StreamResponse?> GetStreamByIdAsync(Identifier streamId, CancellationToken token = default)
    {
        var message = TcpMessageStreamHelpers.GetBytesFromIdentifier(streamId);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.GET_STREAM_CODE);

        var responseBuffer = await SendWithResponseAsync(payload, token);

        if (responseBuffer.Length == 0)
        {
            return null;
        }

        return BinaryMapper.MapStream(responseBuffer);
    }

    public async Task<IReadOnlyList<StreamResponse>> GetStreamsAsync(CancellationToken token = default)
    {
        var message = Array.Empty<byte>();
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.GET_STREAMS_CODE);

        var responseBuffer = await SendWithResponseAsync(payload, token);

        if (responseBuffer.Length == 0)
        {
            return [];
        }

        return BinaryMapper.MapStreams(responseBuffer);
    }

    public async Task UpdateStreamAsync(Identifier streamId, string name, CancellationToken token = default)
    {
        var message = TcpContracts.UpdateStream(streamId, name);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.UPDATE_STREAM_CODE);

        await SendWithResponseAsync(payload, token);
    }

    public async Task PurgeStreamAsync(Identifier streamId, CancellationToken token = default)
    {
        var message = TcpMessageStreamHelpers.GetBytesFromIdentifier(streamId);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.PURGE_STREAM_CODE);

        await SendWithResponseAsync(payload, token);
    }

    public async Task DeleteStreamAsync(Identifier streamId, CancellationToken token = default)
    {
        var message = TcpMessageStreamHelpers.GetBytesFromIdentifier(streamId);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.DELETE_STREAM_CODE);

        await SendWithResponseAsync(payload, token);
    }

    public async Task<IReadOnlyList<TopicResponse>> GetTopicsAsync(Identifier streamId,
        CancellationToken token = default)
    {
        var message = TcpMessageStreamHelpers.GetBytesFromIdentifier(streamId);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.GET_TOPICS_CODE);

        var responseBuffer = await SendWithResponseAsync(payload, token);

        if (responseBuffer.Length == 0)
        {
            return [];
        }

        return BinaryMapper.MapTopics(responseBuffer);
    }

    public async Task<TopicResponse?> GetTopicByIdAsync(Identifier streamId, Identifier topicId,
        CancellationToken token = default)
    {
        var message = TcpContracts.GetTopicById(streamId, topicId);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.GET_TOPIC_CODE);

        var responseBuffer = await SendWithResponseAsync(payload, token);

        if (responseBuffer.Length == 0)
        {
            return null;
        }

        return BinaryMapper.MapTopic(responseBuffer);
    }


    public async Task<TopicResponse?> CreateTopicAsync(Identifier streamId, string name, uint partitionsCount,
        CompressionAlgorithm compressionAlgorithm = CompressionAlgorithm.None, byte? replicationFactor = null,
        ulong messageExpiry = 0, ulong maxTopicSize = 0, CancellationToken token = default)
    {
        var message = TcpContracts.CreateTopic(streamId, name, partitionsCount, compressionAlgorithm,
            replicationFactor, messageExpiry, maxTopicSize);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.CREATE_TOPIC_CODE);

        var responseBuffer = await SendWithResponseAsync(payload, token);

        if (responseBuffer.Length == 0)
        {
            return null;
        }

        return BinaryMapper.MapTopic(responseBuffer);
    }

    public async Task UpdateTopicAsync(Identifier streamId, Identifier topicId, string name,
        CompressionAlgorithm compressionAlgorithm = CompressionAlgorithm.None,
        ulong maxTopicSize = 0, ulong messageExpiry = 0, byte? replicationFactor = null,
        CancellationToken token = default)
    {
        var message = TcpContracts.UpdateTopic(streamId, topicId, name, compressionAlgorithm, maxTopicSize,
            messageExpiry, replicationFactor);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.UPDATE_TOPIC_CODE);

        await SendWithResponseAsync(payload, token);
    }

    public async Task DeleteTopicAsync(Identifier streamId, Identifier topicId, CancellationToken token = default)
    {
        var message = TcpContracts.DeleteTopic(streamId, topicId);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.DELETE_TOPIC_CODE);

        await SendWithResponseAsync(payload, token);
    }

    public async Task PurgeTopicAsync(Identifier streamId, Identifier topicId, CancellationToken token = default)
    {
        var message = TcpContracts.PurgeTopic(streamId, topicId);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.PURGE_TOPIC_CODE);

        await SendWithResponseAsync(payload, token);
    }

    public async Task SendMessagesAsync(Identifier streamId, Identifier topicId, Partitioning partitioning,
        IList<Message> messages,
        CancellationToken token = default)
    {
        var metadataLength = 2 + streamId.Length + 2 + topicId.Length
                             + 2 + partitioning.Length + 4 + 4;
        var messageBufferSize = TcpMessageStreamHelpers.CalculateMessageBytesCount(messages)
                                + metadataLength;
        var payloadBufferSize = messageBufferSize + 4 + BufferSizes.INITIAL_BYTES_LENGTH;

        IMemoryOwner<byte> messageBuffer = MemoryPool<byte>.Shared.Rent(messageBufferSize);
        IMemoryOwner<byte> payloadBuffer = MemoryPool<byte>.Shared.Rent(payloadBufferSize);
        try
        {
            TcpContracts.CreateMessage(messageBuffer.Memory.Span[..messageBufferSize], streamId,
                topicId, partitioning, messages);

            TcpMessageStreamHelpers.CreatePayload(payloadBuffer.Memory.Span[..payloadBufferSize],
                messageBuffer.Memory.Span[..messageBufferSize], CommandCodes.SEND_MESSAGES_CODE);

            await SendWithResponseAsync(payloadBuffer.Memory[..payloadBufferSize].ToArray(), token);
        }
        finally
        {
            messageBuffer.Dispose();
            payloadBuffer.Dispose();
        }
    }

    public async Task FlushUnsavedBufferAsync(Identifier streamId, Identifier topicId, uint partitionId, bool fsync,
        CancellationToken token = default)
    {
        var message = TcpContracts.FlushUnsavedBuffer(streamId, topicId, partitionId, fsync);

        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.FLUSH_UNSAVED_BUFFER_CODE);

        await SendWithResponseAsync(payload, token);
    }

    public async Task<PolledMessages> PollMessagesAsync(Identifier streamId, Identifier topicId, uint? partitionId,
        Consumer consumer,
        PollingStrategy pollingStrategy, uint count, bool autoCommit, CancellationToken token = default)
    {
        var messageBufferSize = CalculateMessageBufferSize(streamId, topicId, consumer);
        var payloadBufferSize = CalculatePayloadBufferSize(messageBufferSize);
        var message = new byte[messageBufferSize];
        var payload = new byte[payloadBufferSize];

        TcpContracts.GetMessages(message.AsSpan()[..messageBufferSize], consumer, streamId,
            topicId, pollingStrategy, count, autoCommit, partitionId);
        TcpMessageStreamHelpers.CreatePayload(payload, message.AsSpan()[..messageBufferSize],
            CommandCodes.POLL_MESSAGES_CODE);

        var responseBuffer = await SendWithResponseAsync(payload, token);

        return BinaryMapper.MapMessages(responseBuffer);
    }

    public async Task StoreOffsetAsync(Consumer consumer, Identifier streamId, Identifier topicId, ulong offset,
        uint? partitionId, CancellationToken token = default)
    {
        var message = TcpContracts.UpdateOffset(streamId, topicId, consumer, offset, partitionId);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.STORE_CONSUMER_OFFSET_CODE);

        await SendWithResponseAsync(payload, token);
    }

    public async Task<OffsetResponse?> GetOffsetAsync(Consumer consumer, Identifier streamId, Identifier topicId,
        uint? partitionId, CancellationToken token = default)
    {
        var message = TcpContracts.GetOffset(streamId, topicId, consumer, partitionId);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.GET_CONSUMER_OFFSET_CODE);

        var responseBuffer = await SendWithResponseAsync(payload, token);

        if (responseBuffer.Length == 0)
        {
            return null;
        }

        return BinaryMapper.MapOffsets(responseBuffer);
    }

    public async Task DeleteOffsetAsync(Consumer consumer, Identifier streamId, Identifier topicId, uint? partitionId,
        CancellationToken token = default)
    {
        var message = TcpContracts.DeleteOffset(streamId, topicId, consumer, partitionId);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.DELETE_CONSUMER_OFFSET_CODE);

        await SendWithResponseAsync(payload, token);
    }

    public async Task<IReadOnlyList<ConsumerGroupResponse>> GetConsumerGroupsAsync(Identifier streamId,
        Identifier topicId,
        CancellationToken token = default)
    {
        var message = TcpContracts.GetGroups(streamId, topicId);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.GET_CONSUMER_GROUPS_CODE);

        var responseBuffer = await SendWithResponseAsync(payload, token);

        if (responseBuffer.Length == 0)
        {
            return [];
        }

        return BinaryMapper.MapConsumerGroups(responseBuffer);
    }

    public async Task<ConsumerGroupResponse?> GetConsumerGroupByIdAsync(Identifier streamId, Identifier topicId,
        Identifier groupId, CancellationToken token = default)
    {
        var message = TcpContracts.GetGroup(streamId, topicId, groupId);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.GET_CONSUMER_GROUP_CODE);

        var responseBuffer = await SendWithResponseAsync(payload, token);

        if (responseBuffer.Length == 0)
        {
            return null;
        }

        return BinaryMapper.MapConsumerGroup(responseBuffer);
    }

    public async Task<ConsumerGroupResponse?> CreateConsumerGroupAsync(Identifier streamId, Identifier topicId,
        string name, CancellationToken token = default)
    {
        var message = TcpContracts.CreateGroup(streamId, topicId, name);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.CREATE_CONSUMER_GROUP_CODE);

        var responseBuffer = await SendWithResponseAsync(payload, token);

        if (responseBuffer.Length == 0)
        {
            return null;
        }

        return BinaryMapper.MapConsumerGroup(responseBuffer);
    }

    public async Task DeleteConsumerGroupAsync(Identifier streamId, Identifier topicId, Identifier groupId,
        CancellationToken token = default)
    {
        var message = TcpContracts.DeleteGroup(streamId, topicId, groupId);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.DELETE_CONSUMER_GROUP_CODE);

        await SendWithResponseAsync(payload, token);
    }

    public async Task JoinConsumerGroupAsync(Identifier streamId, Identifier topicId, Identifier groupId,
        CancellationToken token = default)
    {
        var message = TcpContracts.JoinGroup(streamId, topicId, groupId);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.JOIN_CONSUMER_GROUP_CODE);

        await SendWithResponseAsync(payload, token);
    }

    public async Task LeaveConsumerGroupAsync(Identifier streamId, Identifier topicId, Identifier groupId,
        CancellationToken token = default)
    {
        var message = TcpContracts.LeaveGroup(streamId, topicId, groupId);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.LEAVE_CONSUMER_GROUP_CODE);

        await SendWithResponseAsync(payload, token);
    }

    public async Task DeletePartitionsAsync(Identifier streamId, Identifier topicId, uint partitionsCount,
        CancellationToken token = default)
    {
        var message = TcpContracts.DeletePartitions(streamId, topicId, partitionsCount);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.DELETE_PARTITIONS_CODE);

        await SendWithResponseAsync(payload, token);
    }

    public async Task CreatePartitionsAsync(Identifier streamId, Identifier topicId, uint partitionsCount,
        CancellationToken token = default)
    {
        var message = TcpContracts.CreatePartitions(streamId, topicId, partitionsCount);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.CREATE_PARTITIONS_CODE);

        await SendWithResponseAsync(payload, token);
    }

    public async Task<ClientResponse?> GetMeAsync(CancellationToken token = default)
    {
        var message = Array.Empty<byte>();
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.GET_ME_CODE);

        var responseBuffer = await SendWithResponseAsync(payload, token);

        if (responseBuffer.Length == 0)
        {
            return null;
        }

        return BinaryMapper.MapClient(responseBuffer);
    }

    public async Task<StatsResponse?> GetStatsAsync(CancellationToken token = default)
    {
        var message = Array.Empty<byte>();
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.GET_STATS_CODE);

        var responseBuffer = await SendWithResponseAsync(payload, token);

        if (responseBuffer.Length == 0)
        {
            return null;
        }

        return BinaryMapper.MapStats(responseBuffer);
    }

    public async Task PingAsync(CancellationToken token = default)
    {
        var message = Array.Empty<byte>();
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.PING_CODE);

        await SendWithResponseAsync(payload, token);
    }

    public async Task<IReadOnlyList<ClientResponse>> GetClientsAsync(CancellationToken token = default)
    {
        var message = Array.Empty<byte>();
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.GET_CLIENTS_CODE);

        var responseBuffer = await SendWithResponseAsync(payload, token);

        if (responseBuffer.Length == 0)
        {
            return [];
        }

        return BinaryMapper.MapClients(responseBuffer);
    }

    public async Task<ClientResponse?> GetClientByIdAsync(uint clientId, CancellationToken token = default)
    {
        var message = TcpContracts.GetClient(clientId);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.GET_CLIENT_CODE);

        var responseBuffer = await SendWithResponseAsync(payload, token);

        if (responseBuffer.Length == 0)
        {
            return null;
        }

        return BinaryMapper.MapClient(responseBuffer);
    }

    public async Task<UserResponse?> GetUser(Identifier userId, CancellationToken token = default)
    {
        var message = TcpContracts.GetUser(userId);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.GET_USER_CODE);

        var responseBuffer = await SendWithResponseAsync(payload, token);

        if (responseBuffer.Length == 0)
        {
            return null;
        }

        return BinaryMapper.MapUser(responseBuffer);
    }

    public async Task<IReadOnlyList<UserResponse>> GetUsers(CancellationToken token = default)
    {
        var message = Array.Empty<byte>();
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.GET_USERS_CODE);

        var responseBuffer = await SendWithResponseAsync(payload, token);

        if (responseBuffer.Length == 0)
        {
            return [];
        }

        return BinaryMapper.MapUsers(responseBuffer);
    }

    public async Task<UserResponse?> CreateUser(string userName, string password, UserStatus status,
        Permissions? permissions = null, CancellationToken token = default)
    {
        var message = TcpContracts.CreateUser(userName, password, status, permissions);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.CREATE_USER_CODE);

        var responseBuffer = await SendWithResponseAsync(payload, token);

        if (responseBuffer.Length == 0)
        {
            return null;
        }

        return BinaryMapper.MapUser(responseBuffer);
    }

    public async Task DeleteUser(Identifier userId, CancellationToken token = default)
    {
        var message = TcpContracts.DeleteUser(userId);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.DELETE_USER_CODE);

        await SendWithResponseAsync(payload, token);
    }

    public async Task UpdateUser(Identifier userId, string? userName = null, UserStatus? status = null,
        CancellationToken token = default)
    {
        var message = TcpContracts.UpdateUser(userId, userName, status);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.UPDATE_USER_CODE);

        await SendWithResponseAsync(payload, token);
    }

    public async Task UpdatePermissions(Identifier userId, Permissions? permissions = null,
        CancellationToken token = default)
    {
        var message = TcpContracts.UpdatePermissions(userId, permissions);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.UPDATE_PERMISSIONS_CODE);

        await SendWithResponseAsync(payload, token);
    }

    public async Task ChangePassword(Identifier userId, string currentPassword, string newPassword,
        CancellationToken token = default)
    {
        var message = TcpContracts.ChangePassword(userId, currentPassword, newPassword);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.CHANGE_PASSWORD_CODE);

        await SendWithResponseAsync(payload, token);
    }

    public async Task<AuthResponse?> LoginUser(string userName, string password, CancellationToken token = default)
    {
        var message = TcpContracts.LoginUser(userName, password, "0.5.0", "csharp-sdk");
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.LOGIN_USER_CODE);

        var responseBuffer = await SendWithResponseAsync(payload, token);

        if (responseBuffer.Length <= 0)
        {
            return null;
        }

        var userId = BinaryPrimitives.ReadInt32LittleEndian(responseBuffer.AsSpan()[..responseBuffer.Length]);

        return new AuthResponse(userId, null);
    }

    public async Task LogoutUser(CancellationToken token = default)
    {
        var message = Array.Empty<byte>();
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.LOGOUT_USER_CODE);

        await SendWithResponseAsync(payload, token);
    }

    public async Task<IReadOnlyList<PersonalAccessTokenResponse>> GetPersonalAccessTokensAsync(
        CancellationToken token = default)
    {
        var message = Array.Empty<byte>();
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.GET_PERSONAL_ACCESS_TOKENS_CODE);

        var responseBuffer = await SendWithResponseAsync(payload, token);

        if (responseBuffer.Length == 0)
        {
            return [];
        }

        return BinaryMapper.MapPersonalAccessTokens(responseBuffer);
    }

    public async Task<RawPersonalAccessToken?> CreatePersonalAccessTokenAsync(string name, ulong? expiry = 0,
        CancellationToken token = default)
    {
        var message = TcpContracts.CreatePersonalAccessToken(name, expiry);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.CREATE_PERSONAL_ACCESS_TOKEN_CODE);

        var responseBuffer = await SendWithResponseAsync(payload, token);

        if (responseBuffer.Length == 0)
        {
            return null;
        }

        return BinaryMapper.MapRawPersonalAccessToken(responseBuffer);
    }

    public async Task DeletePersonalAccessTokenAsync(string name, CancellationToken token = default)
    {
        var message = TcpContracts.DeletePersonalRequestToken(name);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.DELETE_PERSONAL_ACCESS_TOKEN_CODE);

        await SendWithResponseAsync(payload, token);
    }

    public async Task<AuthResponse?> LoginWithPersonalAccessToken(string token, CancellationToken ct = default)
    {
        var message = TcpContracts.LoginWithPersonalAccessToken(token);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE);

        var responseBuffer = await SendWithResponseAsync(payload, ct);

        if (responseBuffer.Length <= 1)
        {
            return null;
        }

        var userId = BinaryPrimitives.ReadInt32LittleEndian(responseBuffer.AsSpan()[..4]);

        //TODO: Figure out how to solve this workaround about default of TokenInfo
        return new AuthResponse(userId, default);
    }

    private async Task<byte[]> SendWithResponseAsync(byte[] payload, CancellationToken token = default)
    {
        try
        {
            await _semaphore.WaitAsync(token);

            await _stream.SendAsync(payload, token);
            await _stream.FlushAsync(token);

            // Read the 8-byte header (4 bytes status + 4 bytes length)
            var buffer = new byte[BufferSizes.EXPECTED_RESPONSE_SIZE];
            var totalRead = 0;
            while (totalRead < BufferSizes.EXPECTED_RESPONSE_SIZE)
            {
                var readBytes = await _stream.ReadAsync(buffer.AsMemory(totalRead, BufferSizes.EXPECTED_RESPONSE_SIZE - totalRead), token);
                if (readBytes == 0)
                {
                    throw new InvalidResponseException("Connection closed while reading response header");
                }
                totalRead += readBytes;
            }

            var response = TcpMessageStreamHelpers.GetResponseLengthAndStatus(buffer);

            if (response.Status != 0)
            {
                if (response.Length == 0)
                {
                    throw new InvalidResponseException($"Invalid response status code: {response.Status}");
                }

                var errorBuffer = new byte[response.Length];
                totalRead = 0;
                while (totalRead < response.Length)
                {
                    var readBytes = await _stream.ReadAsync(errorBuffer.AsMemory(totalRead, response.Length - totalRead), token);
                    if (readBytes == 0)
                    {
                        throw new InvalidResponseException($"Connection closed while reading error message. Expected {response.Length} bytes, got {totalRead}");
                    }
                    totalRead += readBytes;
                }
                throw new InvalidResponseException(Encoding.UTF8.GetString(errorBuffer));
            }

            if (response.Length == 0)
            {
                return [];
            }

            // Read the full payload
            var responseBuffer = new byte[response.Length];
            totalRead = 0;
            while (totalRead < response.Length)
            {
                var readBytes = await _stream.ReadAsync(responseBuffer.AsMemory(totalRead, response.Length - totalRead), token);
                if (readBytes == 0)
                {
                    throw new InvalidResponseException($"Connection closed while reading response payload. Expected {response.Length} bytes, got {totalRead}");
                }
                totalRead += readBytes;
            }

            return responseBuffer;
        }
        finally
        {
            _semaphore.Release();
        }
    }

    private static int CalculatePayloadBufferSize(int messageBufferSize)
    {
        return messageBufferSize + 4 + BufferSizes.INITIAL_BYTES_LENGTH;
    }

    private static int CalculateMessageBufferSize(Identifier streamId, Identifier topicId, Consumer consumer)
    {
        // Original: 14 + 5 + 2 + streamId.Length + 2 + topicId.Length + 2 + consumer.Id.Length
        // Added 1 byte for partition flag
        return 15 + 5 + 2 + streamId.Length + 2 + topicId.Length + 2 + consumer.ConsumerId.Length;
    }
}
