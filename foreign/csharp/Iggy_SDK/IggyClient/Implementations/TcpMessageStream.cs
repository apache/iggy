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
using System.Net.Security;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Apache.Iggy.Configuration;
using Apache.Iggy.ConnectionStream;
using Apache.Iggy.Contracts;
using Apache.Iggy.Contracts.Auth;
using Apache.Iggy.Contracts.Tcp;
using Apache.Iggy.Encryption;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Kinds;
using Apache.Iggy.Mappers;
using Apache.Iggy.Messages;
using Apache.Iggy.Utils;
using Microsoft.Extensions.Logging;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.IggyClient.Implementations;

/// <summary>
///     A TCP client for interacting with the Iggy server.
/// </summary>
public sealed class TcpMessageStream : IIggyClient
{
    private const int InvalidCommandStatus = 3;

    private static readonly HashSet<uint> SessionControlCodes =
    [
        CommandCodes.LOGIN_USER_CODE,
        CommandCodes.LOGOUT_USER_CODE,
        CommandCodes.LOGIN_REGISTER_CODE,
        CommandCodes.LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE,
        CommandCodes.LOGIN_REGISTER_WITH_PAT_CODE
    ];

    private readonly IggyClientConfigurator _configuration;
    private readonly EventAggregator<ConnectionStateChangedEventArgs> _connectionEvents;
    private readonly SemaphoreSlim _connectionSemaphore;
    private readonly ILogger<TcpMessageStream> _logger;
    private readonly byte[] _responseHeaderBuffer = new byte[BufferSizes.EXPECTED_RESPONSE_SIZE];
    private readonly SemaphoreSlim _sendingSemaphore;
    private string _currentAddress = string.Empty;
    private X509Certificate2Collection _customCaStore = [];
    private bool _isConnecting;
    private DateTimeOffset _lastConnectionTime;
    private ConnectionState _state = ConnectionState.Disconnected;
    private TcpConnectionStream _stream = null!;

    internal TcpMessageStream(IggyClientConfigurator configuration, ILoggerFactory loggerFactory)
    {
        _configuration = configuration;
        _logger = loggerFactory.CreateLogger<TcpMessageStream>();
        _sendingSemaphore = new SemaphoreSlim(1, 1);
        _connectionSemaphore = new SemaphoreSlim(1, 1);
        _lastConnectionTime = DateTimeOffset.MinValue;
        _connectionEvents = new EventAggregator<ConnectionStateChangedEventArgs>(loggerFactory);
    }

    /// <summary>
    ///     Closes the underlying stream and releases the connection's semaphores.
    /// </summary>
    public void Dispose()
    {
        _stream?.Close();
        _stream?.Dispose();
        _sendingSemaphore.Dispose();
        _connectionSemaphore.Dispose();
        _connectionEvents.Clear();
    }

    /// <inheritdoc />
    public void SubscribeConnectionEvents(Func<ConnectionStateChangedEventArgs, Task> callback)
    {
        _connectionEvents.Subscribe(callback);
    }

    /// <inheritdoc />
    public void UnsubscribeConnectionEvents(Func<ConnectionStateChangedEventArgs, Task> callback)
    {
        _connectionEvents.Unsubscribe(callback);
    }

    /// <inheritdoc />
    public IMessageEncryptor? MessageEncryptor => _configuration.MessageEncryptor;

    /// <inheritdoc />
    public string GetCurrentAddress()
    {
        return _currentAddress;
    }

    /// <inheritdoc />
    public async Task<StreamResponse?> CreateStreamAsync(string name, CancellationToken token = default)
    {
        var message = TcpContracts.CreateStream(name);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.CREATE_STREAM_CODE);

        using IMemoryOwner<byte> responseBuffer = await SendWithResponseAsync(payload, token);

        if (responseBuffer.Memory.Length == 0)
        {
            throw new InvalidResponseException("Received empty response while trying to create stream.");
        }

        return BinaryMapper.MapStream(responseBuffer.Memory.Span);
    }

    /// <inheritdoc />
    public async Task<StreamResponse?> GetStreamByIdAsync(Identifier streamId, CancellationToken token = default)
    {
        var message = TcpMessageStreamHelpers.GetBytesFromIdentifier(streamId);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.GET_STREAM_CODE);

        using IMemoryOwner<byte> responseBuffer = await SendWithResponseAsync(payload, token);

        if (responseBuffer.Memory.Length == 0)
        {
            return null;
        }

        return BinaryMapper.MapStream(responseBuffer.Memory.Span);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<StreamResponse>> GetStreamsAsync(CancellationToken token = default)
    {
        var message = Array.Empty<byte>();
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.GET_STREAMS_CODE);

        using IMemoryOwner<byte> responseBuffer = await SendWithResponseAsync(payload, token);

        if (responseBuffer.Memory.Length == 0)
        {
            return [];
        }

        return BinaryMapper.MapStreams(responseBuffer.Memory.Span);
    }

    /// <inheritdoc />
    public async Task UpdateStreamAsync(Identifier streamId, string name, CancellationToken token = default)
    {
        var message = TcpContracts.UpdateStream(streamId, name);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.UPDATE_STREAM_CODE);

        await SendAckAsync(payload, token);
    }

    /// <inheritdoc />
    public async Task PurgeStreamAsync(Identifier streamId, CancellationToken token = default)
    {
        var message = TcpMessageStreamHelpers.GetBytesFromIdentifier(streamId);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.PURGE_STREAM_CODE);

        await SendAckAsync(payload, token);
    }

    /// <inheritdoc />
    public async Task DeleteStreamAsync(Identifier streamId, CancellationToken token = default)
    {
        var message = TcpMessageStreamHelpers.GetBytesFromIdentifier(streamId);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.DELETE_STREAM_CODE);

        await SendAckAsync(payload, token);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<TopicResponse>> GetTopicsAsync(Identifier streamId,
        CancellationToken token = default)
    {
        var message = TcpMessageStreamHelpers.GetBytesFromIdentifier(streamId);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.GET_TOPICS_CODE);

        using IMemoryOwner<byte> responseBuffer = await SendWithResponseAsync(payload, token);

        if (responseBuffer.Memory.Length == 0)
        {
            return [];
        }

        return BinaryMapper.MapTopics(responseBuffer.Memory.Span);
    }

    /// <inheritdoc />
    public async Task<TopicResponse?> GetTopicByIdAsync(Identifier streamId, Identifier topicId,
        CancellationToken token = default)
    {
        var message = TcpContracts.GetTopicById(streamId, topicId);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.GET_TOPIC_CODE);

        using IMemoryOwner<byte> responseBuffer = await SendWithResponseAsync(payload, token);

        if (responseBuffer.Memory.Length == 0)
        {
            return null;
        }

        return BinaryMapper.MapTopic(responseBuffer.Memory.Span);
    }

    /// <inheritdoc />
    public async Task<TopicResponse?> CreateTopicAsync(Identifier streamId, string name, uint partitionsCount,
        CompressionAlgorithm compressionAlgorithm = CompressionAlgorithm.None, byte? replicationFactor = null,
        TimeSpan? messageExpiry = null, ulong maxTopicSize = 0, CancellationToken token = default)
    {
        var messageExpiryValue = DurationHelpers.ToDuration(messageExpiry);
        var message = TcpContracts.CreateTopic(streamId, name, partitionsCount, compressionAlgorithm,
            replicationFactor, messageExpiryValue, maxTopicSize);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.CREATE_TOPIC_CODE);

        using IMemoryOwner<byte> responseBuffer = await SendWithResponseAsync(payload, token);

        if (responseBuffer.Memory.Length == 0)
        {
            return null;
        }

        return BinaryMapper.MapTopic(responseBuffer.Memory.Span);
    }

    /// <inheritdoc />
    public async Task UpdateTopicAsync(Identifier streamId, Identifier topicId, string name,
        CompressionAlgorithm compressionAlgorithm = CompressionAlgorithm.None,
        ulong maxTopicSize = 0, TimeSpan? messageExpiry = null, byte? replicationFactor = null,
        CancellationToken token = default)
    {
        var messageExpiryValue = DurationHelpers.ToDuration(messageExpiry);
        var message = TcpContracts.UpdateTopic(streamId, topicId, name, compressionAlgorithm, maxTopicSize,
            messageExpiryValue, replicationFactor);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.UPDATE_TOPIC_CODE);

        await SendAckAsync(payload, token);
    }

    /// <inheritdoc />
    public async Task DeleteTopicAsync(Identifier streamId, Identifier topicId, CancellationToken token = default)
    {
        var message = TcpContracts.DeleteTopic(streamId, topicId);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.DELETE_TOPIC_CODE);

        await SendAckAsync(payload, token);
    }

    /// <inheritdoc />
    public async Task PurgeTopicAsync(Identifier streamId, Identifier topicId, CancellationToken token = default)
    {
        var message = TcpContracts.PurgeTopic(streamId, topicId);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.PURGE_TOPIC_CODE);

        await SendAckAsync(payload, token);
    }


    /// <inheritdoc />
    public Task SendMessagesAsync(Identifier streamId, Identifier topicId, Partitioning partitioning,
        IList<Message> messages, CancellationToken token = default)
    {
        return SendMessagesCoreAsync(streamId, topicId, partitioning, AsSpan(messages), token);
    }

    /// <inheritdoc />
    public Task SendMessagesAsync(Identifier streamId, Identifier topicId, Partitioning partitioning,
        Message message, CancellationToken token = default)
    {
        ReadOnlySpan<Message> span = [message];
        return SendMessagesCoreAsync(streamId, topicId, partitioning, span, token);
    }

    /// <inheritdoc />
    public async Task FlushUnsavedBufferAsync(Identifier streamId, Identifier topicId, uint partitionId, bool fsync,
        CancellationToken token = default)
    {
        var message = TcpContracts.FlushUnsavedBuffer(streamId, topicId, partitionId, fsync);

        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.FLUSH_UNSAVED_BUFFER_CODE);

        await SendAckAsync(payload, token);
    }

    /// <inheritdoc />
    public async Task<PolledMessages> PollMessagesAsync(Identifier streamId, Identifier topicId, uint? partitionId,
        Consumer consumer,
        PollingStrategy pollingStrategy, uint count, bool autoCommit, CancellationToken token = default)
    {
        using var rental = await PollMessagesRentedAsync(streamId, topicId, partitionId, consumer, pollingStrategy,
            count, autoCommit, token);
        return BinaryMapper.MaterializeMessages(rental);
    }

    /// <inheritdoc />
    public async Task<PolledMessagesRental> PollMessagesRentedAsync(Identifier streamId, Identifier topicId,
        uint? partitionId,
        Consumer consumer,
        PollingStrategy pollingStrategy, uint count, bool autoCommit, CancellationToken token = default)
    {
        ThrowIfAutoCommitWithEncryptor(autoCommit);

        var messageBufferSize = CalculateMessageBufferSize(streamId, topicId, consumer);
        var payloadBufferSize = CalculatePayloadBufferSize(messageBufferSize);
        var payload = ArrayPool<byte>.Shared.Rent(payloadBufferSize);
        IMemoryOwner<byte>? responseBuffer = null;

        try
        {
            TcpContracts.GetMessages(payload.AsSpan().Slice(8, messageBufferSize), consumer, streamId,
                topicId, pollingStrategy, count, autoCommit, partitionId);
            BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan()[..4], messageBufferSize + 4);
            BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan()[4..8], CommandCodes.POLL_MESSAGES_CODE);

            responseBuffer = await SendWithResponseAsync(payload.AsMemory(0, payloadBufferSize), token);
            return BinaryMapper.MapRentedMessages(responseBuffer.Memory, responseBuffer,
                _configuration.MessageEncryptor);
        }
        catch
        {
            responseBuffer?.Dispose();
            throw;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(payload);
        }
    }

    /// <inheritdoc />
    public async Task StoreOffsetAsync(Consumer consumer, Identifier streamId, Identifier topicId, ulong offset,
        uint? partitionId, CancellationToken token = default)
    {
        var message = TcpContracts.UpdateOffset(streamId, topicId, consumer, offset, partitionId);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.STORE_CONSUMER_OFFSET_CODE);

        await SendAckAsync(payload, token);
    }

    /// <inheritdoc />
    public async Task<OffsetResponse?> GetOffsetAsync(Consumer consumer, Identifier streamId, Identifier topicId,
        uint? partitionId, CancellationToken token = default)
    {
        var message = TcpContracts.GetOffset(streamId, topicId, consumer, partitionId);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.GET_CONSUMER_OFFSET_CODE);

        using IMemoryOwner<byte> responseBuffer = await SendWithResponseAsync(payload, token);

        if (responseBuffer.Memory.Length == 0)
        {
            return null;
        }

        return BinaryMapper.MapOffsets(responseBuffer.Memory.Span);
    }

    /// <inheritdoc />
    public async Task DeleteOffsetAsync(Consumer consumer, Identifier streamId, Identifier topicId, uint? partitionId,
        CancellationToken token = default)
    {
        var message = TcpContracts.DeleteOffset(streamId, topicId, consumer, partitionId);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.DELETE_CONSUMER_OFFSET_CODE);

        await SendAckAsync(payload, token);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<ConsumerGroupResponse>> GetConsumerGroupsAsync(Identifier streamId,
        Identifier topicId,
        CancellationToken token = default)
    {
        var message = TcpContracts.GetGroups(streamId, topicId);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.GET_CONSUMER_GROUPS_CODE);

        using IMemoryOwner<byte> responseBuffer = await SendWithResponseAsync(payload, token);

        if (responseBuffer.Memory.Length == 0)
        {
            return [];
        }

        return BinaryMapper.MapConsumerGroups(responseBuffer.Memory.Span);
    }

    /// <inheritdoc />
    public async Task<ConsumerGroupResponse?> GetConsumerGroupByIdAsync(Identifier streamId, Identifier topicId,
        Identifier groupId, CancellationToken token = default)
    {
        var message = TcpContracts.GetGroup(streamId, topicId, groupId);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.GET_CONSUMER_GROUP_CODE);

        using IMemoryOwner<byte> responseBuffer = await SendWithResponseAsync(payload, token);

        if (responseBuffer.Memory.Length == 0)
        {
            return null;
        }

        return BinaryMapper.MapConsumerGroup(responseBuffer.Memory.Span);
    }

    /// <inheritdoc />
    public async Task<ConsumerGroupResponse?> CreateConsumerGroupAsync(Identifier streamId, Identifier topicId,
        string name, CancellationToken token = default)
    {
        var message = TcpContracts.CreateGroup(streamId, topicId, name);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.CREATE_CONSUMER_GROUP_CODE);

        using IMemoryOwner<byte> responseBuffer = await SendWithResponseAsync(payload, token);

        if (responseBuffer.Memory.Length == 0)
        {
            return null;
        }

        return BinaryMapper.MapConsumerGroup(responseBuffer.Memory.Span);
    }

    /// <inheritdoc />
    public async Task DeleteConsumerGroupAsync(Identifier streamId, Identifier topicId, Identifier groupId,
        CancellationToken token = default)
    {
        var message = TcpContracts.DeleteGroup(streamId, topicId, groupId);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.DELETE_CONSUMER_GROUP_CODE);

        await SendAckAsync(payload, token);
    }

    /// <inheritdoc />
    public async Task JoinConsumerGroupAsync(Identifier streamId, Identifier topicId, Identifier groupId,
        CancellationToken token = default)
    {
        var message = TcpContracts.JoinGroup(streamId, topicId, groupId);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.JOIN_CONSUMER_GROUP_CODE);

        await SendAckAsync(payload, token);
    }

    /// <inheritdoc />
    public async Task LeaveConsumerGroupAsync(Identifier streamId, Identifier topicId, Identifier groupId,
        CancellationToken token = default)
    {
        var message = TcpContracts.LeaveGroup(streamId, topicId, groupId);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.LEAVE_CONSUMER_GROUP_CODE);

        await SendAckAsync(payload, token);
    }

    /// <inheritdoc />
    public async Task DeletePartitionsAsync(Identifier streamId, Identifier topicId, uint partitionsCount,
        CancellationToken token = default)
    {
        var message = TcpContracts.DeletePartitions(streamId, topicId, partitionsCount);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.DELETE_PARTITIONS_CODE);

        await SendAckAsync(payload, token);
    }

    /// <inheritdoc />
    public async Task CreatePartitionsAsync(Identifier streamId, Identifier topicId, uint partitionsCount,
        CancellationToken token = default)
    {
        var message = TcpContracts.CreatePartitions(streamId, topicId, partitionsCount);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.CREATE_PARTITIONS_CODE);

        await SendAckAsync(payload, token);
    }

    /// <inheritdoc />
    public async Task DeleteSegmentsAsync(Identifier streamId, Identifier topicId, uint partitionId,
        uint segmentsCount, CancellationToken token = default)
    {
        var message = TcpContracts.DeleteSegments(streamId, topicId, partitionId, segmentsCount);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.DELETE_SEGMENTS_CODE);

        await SendAckAsync(payload, token);
    }

    /// <inheritdoc />
    public async Task<ClientResponse?> GetMeAsync(CancellationToken token = default)
    {
        var message = Array.Empty<byte>();
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.GET_ME_CODE);

        using IMemoryOwner<byte> responseBuffer = await SendWithResponseAsync(payload, token);

        if (responseBuffer.Memory.Length == 0)
        {
            return null;
        }

        return BinaryMapper.MapClient(responseBuffer.Memory.Span);
    }

    /// <inheritdoc />
    public async Task<StatsResponse?> GetStatsAsync(CancellationToken token = default)
    {
        var message = Array.Empty<byte>();
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.GET_STATS_CODE);

        using IMemoryOwner<byte> responseBuffer = await SendWithResponseAsync(payload, token);

        if (responseBuffer.Memory.Length == 0)
        {
            return null;
        }

        return BinaryMapper.MapStats(responseBuffer.Memory.Span);
    }

    /// <inheritdoc />
    public async Task<ClusterMetadata?> GetClusterMetadataAsync(CancellationToken token = default)
    {
        var message = Array.Empty<byte>();
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.GET_CLUSTER_METADATA_CODE);

        using IMemoryOwner<byte> responseBuffer = await SendWithResponseAsync(payload, token);

        if (responseBuffer.Memory.Length == 0)
        {
            return null;
        }

        return BinaryMapper.MapClusterMetadata(responseBuffer.Memory.Span);
    }

    /// <inheritdoc />
    public async Task PingAsync(CancellationToken token = default)
    {
        var message = Array.Empty<byte>();
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.PING_CODE);

        await SendAckAsync(payload, token);
    }

    /// <inheritdoc />
    public async Task<byte[]> GetSnapshotAsync(SnapshotCompression compression,
        IList<SystemSnapshotType> snapshotTypes, CancellationToken token = default)
    {
        var message = TcpContracts.GetSnapshot(compression, snapshotTypes);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.GET_SNAPSHOT_CODE);

        using IMemoryOwner<byte> result = await SendWithResponseAsync(payload, token);

        return result.Memory.Span.ToArray();
    }

    /// <inheritdoc />
    public async Task<byte[]> SendRawWithResponseAsync(uint code, byte[] payload, CancellationToken token = default)
    {
        if (SessionControlCodes.Contains(code))
        {
            throw new IggyInvalidStatusCodeException(InvalidCommandStatus,
                $"Invalid response status code: {InvalidCommandStatus}");
        }

        var buffer = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + payload.Length];
        TcpMessageStreamHelpers.CreatePayload(buffer, payload, (int)code);

        using IMemoryOwner<byte> result = await SendWithResponseAsync(buffer, token);

        return result.Memory.Length <= 1 ? [] : result.Memory.Span.ToArray();
    }

    /// <inheritdoc />
    public async Task ConnectAsync(CancellationToken token = default)
    {
        if (_state is ConnectionState.Connected
            or ConnectionState.Authenticating
            or ConnectionState.Authenticated)
        {
            _logger.LogWarning("Connection is already connected");
            return;
        }

        if (_lastConnectionTime != DateTimeOffset.MinValue)
        {
            await Task.Delay(_configuration.ReconnectionSettings.InitialDelay, token);
        }

        SetConnectionStateAsync(ConnectionState.Connecting);
        _isConnecting = true;
        try
        {
            await TryEstablishConnectionAsync(token);
        }
        finally
        {
            _isConnecting = false;
        }
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<ClientResponse>> GetClientsAsync(CancellationToken token = default)
    {
        var message = Array.Empty<byte>();
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.GET_CLIENTS_CODE);

        using IMemoryOwner<byte> responseBuffer = await SendWithResponseAsync(payload, token);

        if (responseBuffer.Memory.Length == 0)
        {
            return [];
        }

        return BinaryMapper.MapClients(responseBuffer.Memory.Span);
    }

    /// <inheritdoc />
    public async Task<ClientResponse?> GetClientByIdAsync(uint clientId, CancellationToken token = default)
    {
        var message = TcpContracts.GetClient(clientId);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.GET_CLIENT_CODE);

        using IMemoryOwner<byte> responseBuffer = await SendWithResponseAsync(payload, token);

        if (responseBuffer.Memory.Length == 0)
        {
            return null;
        }

        return BinaryMapper.MapClient(responseBuffer.Memory.Span);
    }

    /// <inheritdoc />
    public async Task<UserResponse?> GetUserAsync(Identifier userId, CancellationToken token = default)
    {
        var message = TcpContracts.GetUser(userId);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.GET_USER_CODE);

        using IMemoryOwner<byte> responseBuffer = await SendWithResponseAsync(payload, token);

        if (responseBuffer.Memory.Length == 0)
        {
            return null;
        }

        return BinaryMapper.MapUser(responseBuffer.Memory.Span);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<UserResponse>> GetUsersAsync(CancellationToken token = default)
    {
        var message = Array.Empty<byte>();
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.GET_USERS_CODE);

        using IMemoryOwner<byte> responseBuffer = await SendWithResponseAsync(payload, token);

        if (responseBuffer.Memory.Length == 0)
        {
            return [];
        }

        return BinaryMapper.MapUsers(responseBuffer.Memory.Span);
    }

    /// <inheritdoc />
    public async Task<UserResponse?> CreateUserAsync(string userName, string password, UserStatus status,
        Permissions? permissions = null, CancellationToken token = default)
    {
        var message = TcpContracts.CreateUser(userName, password, status, permissions);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.CREATE_USER_CODE);

        using IMemoryOwner<byte> responseBuffer = await SendWithResponseAsync(payload, token);

        if (responseBuffer.Memory.Length == 0)
        {
            return null;
        }

        return BinaryMapper.MapUser(responseBuffer.Memory.Span);
    }

    /// <inheritdoc />
    public async Task DeleteUserAsync(Identifier userId, CancellationToken token = default)
    {
        var message = TcpContracts.DeleteUser(userId);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.DELETE_USER_CODE);

        await SendAckAsync(payload, token);
    }

    /// <inheritdoc />
    public async Task UpdateUserAsync(Identifier userId, string? userName = null, UserStatus? status = null,
        CancellationToken token = default)
    {
        var message = TcpContracts.UpdateUser(userId, userName, status);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.UPDATE_USER_CODE);

        await SendAckAsync(payload, token);
    }

    /// <inheritdoc />
    public async Task UpdatePermissionsAsync(Identifier userId, Permissions? permissions = null,
        CancellationToken token = default)
    {
        var message = TcpContracts.UpdatePermissions(userId, permissions);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.UPDATE_PERMISSIONS_CODE);

        await SendAckAsync(payload, token);
    }

    /// <inheritdoc />
    public async Task ChangePasswordAsync(Identifier userId, string currentPassword, string newPassword,
        CancellationToken token = default)
    {
        var message = TcpContracts.ChangePassword(userId, currentPassword, newPassword);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.CHANGE_PASSWORD_CODE);

        await SendAckAsync(payload, token);
    }

    /// <inheritdoc />
    public async Task<AuthResponse?> LoginUserAsync(string userName, string password, CancellationToken token = default)
    {
        if (_state == ConnectionState.Disconnected)
        {
            throw new NotConnectedException();
        }

        // TODO: Add binary protocol version
        var message = TcpContracts.LoginUser(userName, password, SdkVersion.Value, "csharp-sdk");
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.LOGIN_USER_CODE);

        SetConnectionStateAsync(ConnectionState.Authenticating);
        using IMemoryOwner<byte> responseBuffer = await SendWithResponseAsync(payload, token);

        if (responseBuffer.Memory.Length == 0)
        {
            return null;
        }

        var userId = BinaryPrimitives.ReadInt32LittleEndian(responseBuffer.Memory.Span[..responseBuffer.Memory.Length]);
        SetConnectionStateAsync(ConnectionState.Authenticated);

        if (await RedirectAsync(token))
        {
            await ConnectAsync(token);
            return await LoginUserAsync(userName, password, token);
        }

        var authResponse = new AuthResponse(userId, null);
        return authResponse;
    }

    /// <inheritdoc />
    public async Task LogoutUserAsync(CancellationToken token = default)
    {
        var message = Array.Empty<byte>();
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.LOGOUT_USER_CODE);

        await SendAckAsync(payload, token);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<PersonalAccessTokenResponse>> GetPersonalAccessTokensAsync(
        CancellationToken token = default)
    {
        var message = Array.Empty<byte>();
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.GET_PERSONAL_ACCESS_TOKENS_CODE);

        using IMemoryOwner<byte> responseBuffer = await SendWithResponseAsync(payload, token);

        if (responseBuffer.Memory.Length == 0)
        {
            return [];
        }

        return BinaryMapper.MapPersonalAccessTokens(responseBuffer.Memory.Span);
    }

    /// <inheritdoc />
    public async Task<RawPersonalAccessToken?> CreatePersonalAccessTokenAsync(string name, TimeSpan? expiry = null,
        CancellationToken token = default)
    {
        var message = TcpContracts.CreatePersonalAccessToken(name, DurationHelpers.ToDuration(expiry));
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.CREATE_PERSONAL_ACCESS_TOKEN_CODE);

        using IMemoryOwner<byte> responseBuffer = await SendWithResponseAsync(payload, token);

        if (responseBuffer.Memory.Length == 0)
        {
            return null;
        }

        return BinaryMapper.MapRawPersonalAccessToken(responseBuffer.Memory.Span);
    }

    /// <inheritdoc />
    public async Task DeletePersonalAccessTokenAsync(string name, CancellationToken token = default)
    {
        var message = TcpContracts.DeletePersonalRequestToken(name);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.DELETE_PERSONAL_ACCESS_TOKEN_CODE);

        await SendAckAsync(payload, token);
    }

    /// <inheritdoc />
    public async Task<AuthResponse?> LoginWithPersonalAccessTokenAsync(string token, CancellationToken ct = default)
    {
        var message = TcpContracts.LoginWithPersonalAccessToken(token);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE);

        SetConnectionStateAsync(ConnectionState.Authenticating);
        using IMemoryOwner<byte> responseBuffer = await SendWithResponseAsync(payload, ct);

        if (responseBuffer.Memory.Length == 0)
        {
            return null;
        }

        var userId = BinaryPrimitives.ReadInt32LittleEndian(responseBuffer.Memory.Span[..4]);

        SetConnectionStateAsync(ConnectionState.Authenticated);

        if (await RedirectAsync(ct))
        {
            await ConnectAsync(ct);
            return await LoginWithPersonalAccessTokenAsync(token, ct);
        }

        return new AuthResponse(userId, null);
    }

    // Server-side autoCommit commits the batch offset before the client decrypts, so a decryption failure
    // would permanently skip the whole batch. IggyConsumer guards this too, but the raw poll is public and
    // bypasses that path. Opt out via IggyClientConfigurator.AllowAutoCommitWithEncryptor.
    private void ThrowIfAutoCommitWithEncryptor(bool autoCommit)
    {
        if (autoCommit && _configuration.MessageEncryptor is not null && !_configuration.AllowAutoCommitWithEncryptor)
        {
            throw new InvalidOperationException(
                "AutoCommit with a message encryptor risks silent message loss: the offset is committed before decryption. Poll with autoCommit false, or set AllowAutoCommitWithEncryptor.");
        }
    }

    private Task SendMessagesCoreAsync(Identifier streamId, Identifier topicId, Partitioning partitioning,
        ReadOnlySpan<Message> messages, CancellationToken token)
    {
        var encryptor = _configuration.MessageEncryptor;

        // With an encryptor this is an upper bound; the fill reports the size actually written and only that
        // prefix is sent.
        var metadataLength = 2 + streamId.Length + 2 + topicId.Length
                             + 2 + partitioning.Length + 4 + 4;
        var maxMessageBufferSize = TcpMessageStreamHelpers.CalculateMessageBytesCount(messages, encryptor)
                                   + metadataLength;
        var maxPayloadBufferSize = CalculatePayloadBufferSize(maxMessageBufferSize);

        IMemoryOwner<byte> payloadBuffer = MemoryPool<byte>.Shared.Rent(maxPayloadBufferSize);
        int payloadBufferSize;
        try
        {
            var messageBufferSize = FillSendMessagesPayload(payloadBuffer.Memory.Span, maxMessageBufferSize,
                streamId, topicId, partitioning, messages, encryptor);
            payloadBufferSize = CalculatePayloadBufferSize(messageBufferSize);
        }
        catch
        {
            payloadBuffer.Dispose();
            throw;
        }

        return SendAckAndDisposeAsync(payloadBuffer, payloadBufferSize, token);
    }

    private async Task SendAckAndDisposeAsync(IMemoryOwner<byte> payloadBuffer, int payloadBufferSize,
        CancellationToken token)
    {
        try
        {
            await SendAckAsync(payloadBuffer.Memory[..payloadBufferSize], token);
        }
        finally
        {
            payloadBuffer.Dispose();
        }
    }

    private static ReadOnlySpan<Message> AsSpan(IList<Message> messages)
    {
        return messages switch
        {
            Message[] array => array,
            List<Message> list => CollectionsMarshal.AsSpan(list),
            _ => messages.ToArray()
        };
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int FillSendMessagesPayload(Span<byte> buffer, int maxMessageBufferSize,
        Identifier streamId, Identifier topicId, Partitioning partitioning, ReadOnlySpan<Message> messages,
        IMessageEncryptor? encryptor)
    {
        var messageBufferSize = TcpContracts.CreateMessage(buffer.Slice(8, maxMessageBufferSize), streamId, topicId,
            partitioning, messages, encryptor);
        BinaryPrimitives.WriteInt32LittleEndian(buffer[..4], messageBufferSize + 4);
        BinaryPrimitives.WriteInt32LittleEndian(buffer[4..8], CommandCodes.SEND_MESSAGES_CODE);
        return messageBufferSize;
    }

    private async Task TryEstablishConnectionAsync(CancellationToken token)
    {
        var retryCount = 0;
        var delay = _configuration.ReconnectionSettings.InitialDelay;
        do
        {
            // ReSharper disable once ConditionIsAlwaysTrueOrFalseAccordingToNullableAPIContract
            _stream?.Close();
            _stream?.Dispose();

            if (string.IsNullOrEmpty(_currentAddress))
            {
                _currentAddress = _configuration.BaseAddress;
            }

            var urlPortSplitter = _currentAddress.Split(":");
            if (urlPortSplitter.Length > 2)
            {
                throw new InvalidBaseAddressException();
            }

            try
            {
                var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                socket.SendBufferSize = _configuration.SendBufferSize;
                socket.ReceiveBufferSize = _configuration.ReceiveBufferSize;

                await socket.ConnectAsync(urlPortSplitter[0], int.Parse(urlPortSplitter[1]), token);

                socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
                socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveTime, 5);

                SetConnectionStateAsync(ConnectionState.Connected);
                _lastConnectionTime = DateTimeOffset.UtcNow;

                _stream = _configuration.TlsSettings.Enabled switch
                {
                    true => await CreateSslStreamAndAuthenticate(socket, _configuration.TlsSettings),
                    false => new TcpConnectionStream(new NetworkStream(socket, true))
                };

                if (_configuration.AutoLoginSettings.Enabled)
                {
                    _logger.LogInformation("Auto login enabled. Trying to login with credentials: {Username}",
                        _configuration.AutoLoginSettings.Username);
                    await LoginUserAsync(_configuration.AutoLoginSettings.Username,
                        _configuration.AutoLoginSettings.Password, token);
                }

                break;
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Failed to connect");

                if (!_configuration.ReconnectionSettings.Enabled ||
                    (_configuration.ReconnectionSettings.MaxRetries > 0 &&
                     retryCount >= _configuration.ReconnectionSettings.MaxRetries))
                {
                    SetConnectionStateAsync(ConnectionState.Disconnected);
                    throw;
                }

                retryCount++;
                if (_configuration.ReconnectionSettings.UseExponentialBackoff)
                {
                    delay *= _configuration.ReconnectionSettings.BackoffMultiplier;

                    if (delay > _configuration.ReconnectionSettings.MaxDelay)
                    {
                        delay = _configuration.ReconnectionSettings.MaxDelay;
                    }
                }

                if (_logger.IsEnabled(LogLevel.Information))
                {
                    _logger.LogInformation("Retrying connection attempt {RetryCount} with delay {Delay}", retryCount,
                        delay);
                }

                await Task.Delay(delay, token);
            }
        } while (true);
    }

    private async Task<ClusterNode?> GetCurrentLeaderNodeAsync(CancellationToken token)
    {
        try
        {
            var clusterMetadata = await GetClusterMetadataAsync(token);
            if (clusterMetadata == null)
            {
                return null;
            }

            // Single-node cluster (clustering disabled) - no redirection needed
            if (clusterMetadata.Nodes.Count() == 1)
            {
                return null;
            }

            var leaderNode = clusterMetadata.Nodes.FirstOrDefault(x => x.Role == ClusterNodeRole.Leader);
            if (leaderNode == null)
            {
                throw new MissingLeaderException();
            }

            return leaderNode;
        }
        // todo: change after error refactoring, error code 5 is for feature not supported
        catch (IggyInvalidStatusCodeException e) when (e.StatusCode == 5)
        {
            return null;
        }
    }

    private async Task<TcpConnectionStream> CreateSslStreamAndAuthenticate(Socket socket, TlsSettings tlsSettings)
    {
        ValidateCertificatePath(tlsSettings.CertificatePath);

        _customCaStore = new X509Certificate2Collection();
        _customCaStore.ImportFromPemFile(tlsSettings.CertificatePath);
        var stream = new NetworkStream(socket, true);
        var sslStream = new SslStream(stream, false, RemoteCertificateValidationCallback);

        await sslStream.AuthenticateAsClientAsync(tlsSettings.Hostname);

        return new TcpConnectionStream(sslStream);
    }

    private async Task SendAckAsync(ReadOnlyMemory<byte> payload, CancellationToken token = default)
    {
        using IMemoryOwner<byte> _ = await SendWithResponseAsync(payload, token);
    }

    private async Task<IMemoryOwner<byte>> SendWithResponseAsync(ReadOnlyMemory<byte> payload,
        CancellationToken token = default)
    {
        try
        {
            return await SendRawAsync(payload, token);
        }
        catch (Exception e) when (IsConnectionException(e) && !_isConnecting)
        {
            _logger.LogWarning("Connection lost");
            if (!_configuration.ReconnectionSettings.Enabled)
            {
                _logger.LogWarning("Reconnection is disabled");
                SetConnectionStateAsync(ConnectionState.Disconnected);
                throw;
            }

            return await HandleReconnectionAsync(payload, token);
        }
    }

    private async Task<IMemoryOwner<byte>> HandleReconnectionAsync(ReadOnlyMemory<byte> payload,
        CancellationToken token)
    {
        var currentTime = DateTimeOffset.UtcNow;
        await _connectionSemaphore.WaitAsync(token);

        try
        {
            if (_state is ConnectionState.Connected or ConnectionState.Authenticated
                && _lastConnectionTime > currentTime)
            {
                _logger.LogInformation("Connection already established, sending payload");
                return await SendRawAsync(payload, token);
            }

            SetConnectionStateAsync(ConnectionState.Disconnected);
            _logger.LogInformation("Reconnecting to the server");
            await ConnectAsync(token);

            _logger.LogInformation("Reconnected to the server");

            await Task.Delay(_configuration.ReconnectionSettings.WaitAfterReconnect, token);

            return await SendRawAsync(payload, token);
        }
        finally
        {
            _connectionSemaphore.Release();
        }
    }

    private async Task<IMemoryOwner<byte>> SendRawAsync(ReadOnlyMemory<byte> payload, CancellationToken token)
    {
        if (_state is ConnectionState.Disconnected or ConnectionState.Connecting)
        {
            throw new NotConnectedException();
        }

        await _sendingSemaphore.WaitAsync(token);

        try
        {
            await _stream.SendAsync(payload, token);
            await _stream.FlushAsync(token);

            // Read the 8-byte header (4 bytes status + 4 bytes length)
            var totalRead = 0;
            while (totalRead < BufferSizes.EXPECTED_RESPONSE_SIZE)
            {
                var readBytes
                    = await _stream.ReadAsync(
                        _responseHeaderBuffer.AsMemory(totalRead, BufferSizes.EXPECTED_RESPONSE_SIZE - totalRead),
                        token);
                if (readBytes == 0)
                {
                    throw new IggyZeroBytesException();
                }

                totalRead += readBytes;
            }

            var response = TcpMessageStreamHelpers.GetResponseLengthAndStatus(_responseHeaderBuffer);

            if (response.Status != 0)
            {
                if (response.Length == 0)
                {
                    throw new IggyInvalidStatusCodeException(response.Status,
                        $"Invalid response status code: {response.Status}");
                }


                using var errorBuffer = ArrayPoolHelper.Rent(response.Length);
                totalRead = 0;
                while (totalRead < response.Length)
                {
                    var readBytes
                        = await _stream.ReadAsync(errorBuffer.Memory.Slice(totalRead, response.Length - totalRead),
                            token);
                    if (readBytes == 0)
                    {
                        throw new IggyZeroBytesException();
                    }

                    totalRead += readBytes;
                }

                throw new InvalidResponseException(Encoding.UTF8.GetString(errorBuffer.Memory.Span));
            }

            if (response.Length == 0)
            {
                return EmptyMemoryOwner.Instance;
            }

            var responseBuffer = ArrayPoolHelper.Rent(response.Length);
            try
            {
                totalRead = 0;
                while (totalRead < response.Length)
                {
                    var readBytes
                        = await _stream.ReadAsync(responseBuffer.Memory.Slice(totalRead, response.Length - totalRead),
                            token);

                    if (readBytes == 0)
                    {
                        throw new IggyZeroBytesException();
                    }

                    totalRead += readBytes;
                }
            }
            catch
            {
                responseBuffer.Dispose();
                throw;
            }

            return responseBuffer;
        }
        finally
        {
            _sendingSemaphore.Release();
        }
    }

    private static bool IsConnectionException(Exception ex)
    {
        return ex is IggyZeroBytesException or
            NotConnectedException or
            SocketException or
            IOException or
            ObjectDisposedException;
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

    /// <summary>
    ///     Sets the connection state and publishes a ConnectionStateChangedEventArgs to subscribers via the connection event
    ///     aggregator.
    /// </summary>
    /// <param name="newState">The new connection state</param>
    private void SetConnectionStateAsync(ConnectionState newState)
    {
        if (_state == newState)
        {
            return;
        }

        var previousState = _state;
        _state = newState;

        _logger.LogInformation("Connection state changed: {PreviousState} -> {CurrentState}", previousState, newState);
        _connectionEvents.Publish(new ConnectionStateChangedEventArgs(previousState, newState));
    }

    private void ValidateCertificatePath(string tlsCertificatePath)
    {
        if (string.IsNullOrEmpty(tlsCertificatePath)
            || !File.Exists(tlsCertificatePath))
        {
            throw new InvalidCertificatePathException(tlsCertificatePath);
        }
    }

    private bool RemoteCertificateValidationCallback(object sender, X509Certificate? certificate, X509Chain? chain,
        SslPolicyErrors sslPolicyErrors)
    {
        if (sslPolicyErrors == SslPolicyErrors.None)
        {
            return true;
        }

        if (certificate is null)
        {
            return false;
        }

        if (certificate is not X509Certificate2 serverCert)
        {
            serverCert = new X509Certificate2(certificate);
        }

        if (_customCaStore.Any(ca => ca.Thumbprint == serverCert.Thumbprint))
        {
            if (DateTime.UtcNow <= serverCert.NotAfter && DateTime.UtcNow >= serverCert.NotBefore)
            {
                return true;
            }

            _logger.LogError(
                "Server certificate matches trusted key but is expired. Valid from {NotBefore} to {NotAfter}",
                serverCert.NotBefore, serverCert.NotAfter);
            return false;
        }


        using var customChain = new X509Chain();
        customChain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        customChain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        foreach (var ca in _customCaStore)
        {
            customChain.ChainPolicy.CustomTrustStore.Add(ca);
            customChain.ChainPolicy.ExtraStore.Add(ca);
        }

        customChain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;

        if (customChain.Build(new X509Certificate2(certificate)))
        {
            if (!sslPolicyErrors.HasFlag(SslPolicyErrors.RemoteCertificateNameMismatch))
            {
                return true;
            }

            _logger.LogError("Custom CA chain is valid, but hostname does not match");
            return false;
        }

        foreach (var chainStatus in customChain.ChainStatus)
        {
            _logger.LogWarning("Certificate validation failed: {ChainStatus} - {StatusInformation}", chainStatus.Status,
                chainStatus.StatusInformation);
        }

        return false;
    }

    private async Task<bool> RedirectAsync(CancellationToken token)
    {
        var currentLeaderNode = await GetCurrentLeaderNodeAsync(token);
        if (currentLeaderNode == null)
        {
            return false;
        }

        var leaderAddress = $"{currentLeaderNode.Ip}:{currentLeaderNode.Endpoints.Tcp}";
        if (leaderAddress == _currentAddress)
        {
            return false;
        }

        _currentAddress = leaderAddress;

        _logger.LogInformation("Leader address changed. Trying to reconnect to {Address}",
            leaderAddress);

        _stream.Close();
        SetConnectionStateAsync(ConnectionState.Disconnected);
        return true;
    }

    internal sealed class EmptyMemoryOwner : IMemoryOwner<byte>
    {
        public static readonly EmptyMemoryOwner Instance = new();

        private EmptyMemoryOwner()
        {
        }

        public Memory<byte> Memory => Memory<byte>.Empty;

        public void Dispose()
        {
        }
    }
}

internal static class ArrayPoolHelper
{
    public static SlicedMemoryOwner Rent(int minimumLength, bool clearOnReturn = false)
    {
        return new SlicedMemoryOwner(minimumLength, clearOnReturn);
    }

    internal sealed class SlicedMemoryOwner(int minimumLength, bool clearOnReturn = false) : IMemoryOwner<byte>
    {
        private readonly byte[] _value = ArrayPool<byte>.Shared.Rent(minimumLength);
        private int _disposed;

        public Memory<byte> Memory => _value.AsMemory()[..minimumLength];

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            ArrayPool<byte>.Shared.Return(_value, clearOnReturn);
        }
    }
}
