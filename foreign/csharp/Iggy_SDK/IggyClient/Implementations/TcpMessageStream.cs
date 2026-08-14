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
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security.Cryptography.X509Certificates;
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
using Apache.Iggy.Vsr;
using Microsoft.Extensions.Logging;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.IggyClient.Implementations;

/// <summary>
///     A TCP client for interacting with the Iggy server over the consensus (VSR) framing. The framed request
///     path, leader redirection and register handshake live in <c>TcpMessageStream.Vsr.cs</c>.
/// </summary>
public sealed partial class TcpMessageStream : IIggyClient
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
    private readonly SemaphoreSlim _connectGate = new(1, 1);
    private readonly EventAggregator<ConnectionStateChangedEventArgs> _connectionEvents;
    private readonly SemaphoreSlim _connectionSemaphore;
    private readonly ILogger<TcpMessageStream> _logger;
    private readonly SemaphoreSlim _sendingSemaphore;
    private string _currentAddress = string.Empty;

    // The address the socket actually connected to, as an IP the roster can be compared against.
    // _currentAddress keeps whatever the caller configured (possibly a hostname the roster never
    // mentions), so leader comparisons made against it would move a client that is already on the
    // leader. Written only by the connect loop.
    private string _currentRemoteAddress = string.Empty;
    private X509Certificate2Collection _customCaStore = [];
    private volatile bool _disposed;
    private int _isConnecting;
    private DateTimeOffset _lastConnectionTime;
    private int _leaderRedirectCount;

    // Both are written by the connect and redirect paths, which do not hold the sending semaphore the request
    // paths read them under, so they are accessed through Interlocked rather than as plain fields. Losing an
    // update to the skip flag leaves a connection reporting Connected that never authenticated; losing one to
    // the redirect counter over- or under-spends the redirect budget.
    private int _skipAutoLoginOnce;
    private volatile ConnectionState _state = ConnectionState.Disconnected;
    private TcpConnectionStream _stream = null!;

    private bool IsConnecting => Volatile.Read(ref _isConnecting) != 0;

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
        _disposed = true;
        _stream?.Close();
        _stream?.Dispose();

        SetConnectionStateAsync(ConnectionState.Disconnected);
        _sendingSemaphore.Dispose();
        _connectionSemaphore.Dispose();
        _connectGate.Dispose();
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
        CompressionAlgorithm compressionAlgorithm = CompressionAlgorithm.None,
        TimeSpan? messageExpiry = null, ulong maxTopicSize = 0, CancellationToken token = default)
    {
        var messageExpiryValue = DurationHelpers.ToDuration(messageExpiry);
        var message = TcpContracts.CreateTopic(streamId, name, partitionsCount, compressionAlgorithm,
            messageExpiryValue, maxTopicSize);
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
        ulong maxTopicSize = 0, TimeSpan? messageExpiry = null,
        CancellationToken token = default)
    {
        var messageExpiryValue = DurationHelpers.ToDuration(messageExpiry);
        var message = TcpContracts.UpdateTopic(streamId, topicId, name, compressionAlgorithm, maxTopicSize,
            messageExpiryValue);
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
        _groupState.InvalidatePartitionCount(new TopicKey(streamId, topicId));
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
    public Task<SendMessagesResponse> SendMessagesAsync(Identifier streamId, Identifier topicId,
        Partitioning partitioning, IList<Message> messages, CancellationToken token = default)
    {
        if (NeedsClientSidePartitioning(partitioning))
        {
            return SendMessagesResolvedAsync(streamId, topicId, partitioning, messages, token);
        }

        return SendMessagesCoreAsync(streamId, topicId, partitioning, AsSpan(messages), token);
    }

    /// <inheritdoc />
    public Task<SendMessagesResponse> SendMessagesAsync(Identifier streamId, Identifier topicId,
        Partitioning partitioning, Message message, CancellationToken token = default)
    {
        if (NeedsClientSidePartitioning(partitioning))
        {
            return SendMessagesResolvedAsync(streamId, topicId, partitioning, [message], token);
        }

        ReadOnlySpan<Message> span = [message];
        return SendMessagesCoreAsync(streamId, topicId, partitioning, span, token);
    }

    /// <summary>
    ///     This feature is not supported by the server.
    /// </summary>
    /// <exception cref="FeatureUnavailableException"></exception>
    public Task FlushUnsavedBufferAsync(Identifier streamId, Identifier topicId, uint partitionId, bool fsync,
        CancellationToken token = default)
    {
        throw new FeatureUnavailableException();
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
    public Task<PolledMessagesRental> PollMessagesRentedAsync(Identifier streamId, Identifier topicId,
        uint? partitionId,
        Consumer consumer,
        PollingStrategy pollingStrategy, uint count, bool autoCommit, CancellationToken token = default)
    {
        ThrowIfAutoCommitWithEncryptor(autoCommit);

        // The broker routes explicit partitions only, so a group poll picks one of the member's assigned
        // partitions client-side.
        if (consumer.Type == ConsumerType.ConsumerGroup && partitionId is null)
        {
            return PollGroupMessagesRentedAsync(streamId, topicId, consumer, pollingStrategy, count, autoCommit,
                token);
        }

        return PollPartitionMessagesRentedAsync(streamId, topicId, partitionId, consumer, pollingStrategy, count,
            autoCommit, token);
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
        _groupState.DeregisterGroup(new GroupKey(streamId, topicId, groupId));
    }

    /// <inheritdoc />
    public async Task JoinConsumerGroupAsync(Identifier streamId, Identifier topicId, Identifier groupId,
        CancellationToken token = default)
    {
        var message = TcpContracts.JoinGroup(streamId, topicId, groupId);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.JOIN_CONSUMER_GROUP_CODE);

        await SendAckAsync(payload, token);

        // A join rebalances the group, so whatever this client holds for it is a generation behind and every
        // poll under it would be fenced until the first re-sync.
        _groupState.InvalidateAssignment(new GroupKey(streamId, topicId, groupId));
    }

    /// <inheritdoc />
    public async Task LeaveConsumerGroupAsync(Identifier streamId, Identifier topicId, Identifier groupId,
        CancellationToken token = default)
    {
        var message = TcpContracts.LeaveGroup(streamId, topicId, groupId);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.LEAVE_CONSUMER_GROUP_CODE);

        await SendAckAsync(payload, token);
        _groupState.DeregisterGroup(new GroupKey(streamId, topicId, groupId));
    }

    /// <inheritdoc />
    public async Task DeletePartitionsAsync(Identifier streamId, Identifier topicId, uint partitionsCount,
        CancellationToken token = default)
    {
        var message = TcpContracts.DeletePartitions(streamId, topicId, partitionsCount);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.DELETE_PARTITIONS_CODE);

        await SendAckAsync(payload, token);
        _groupState.InvalidatePartitionCount(new TopicKey(streamId, topicId));
    }

    /// <inheritdoc />
    public async Task CreatePartitionsAsync(Identifier streamId, Identifier topicId, uint partitionsCount,
        CancellationToken token = default)
    {
        var message = TcpContracts.CreatePartitions(streamId, topicId, partitionsCount);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.CREATE_PARTITIONS_CODE);

        await SendAckAsync(payload, token);
        _groupState.InvalidatePartitionCount(new TopicKey(streamId, topicId));
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

        await RefreshGroupAssignmentsAsync(token);
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
    public async Task<byte[]> SendBinaryRequestAsync(uint code, byte[] payload, CancellationToken token = default)
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
    public Task ConnectAsync(CancellationToken token = default)
    {
        return ConnectAsync(true, token);
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

        return await LoginRegisterAsync(CommandCodes.LOGIN_REGISTER_CODE,
            LoginRegister.Serialize(userName, password), token);
    }

    /// <inheritdoc />
    public async Task LogoutUserAsync(CancellationToken token = default)
    {
        var message = Array.Empty<byte>();
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.LOGOUT_USER_CODE);

        try
        {
            await SendAckAsync(payload, token);
        }
        finally
        {
            await ResetConsensusSessionAsync();

            if (_state == ConnectionState.Authenticated)
            {
                SetConnectionStateAsync(ConnectionState.Connected);
            }
        }
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
        return await LoginRegisterAsync(CommandCodes.LOGIN_REGISTER_WITH_PAT_CODE,
            LoginRegister.SerializeWithPersonalAccessToken(token), ct);
    }

    /// <summary>
    ///     Connects, optionally without the configured auto login. A caller that authenticates itself right
    ///     after the connect passes <c>false</c>, so the connect does not spend a round trip on credentials the
    ///     caller is about to replace.
    /// </summary>
    private async Task ConnectAsync(bool autoLogin, CancellationToken token)
    {
        if (_state is ConnectionState.Connected
            or ConnectionState.Authenticating
            or ConnectionState.Authenticated)
        {
            _logger.LogWarning("Connection is already connected");
            return;
        }

        await _connectGate.WaitAsync(token);
        Interlocked.Exchange(ref _isConnecting, 1);
        try
        {
            if (_state is ConnectionState.Connected
                or ConnectionState.Authenticating
                or ConnectionState.Authenticated)
            {
                return;
            }

            if (_lastConnectionTime != DateTimeOffset.MinValue)
            {
                await Task.Delay(_configuration.ReconnectionSettings.InitialDelay, token);
            }

            SetConnectionStateAsync(ConnectionState.Connecting);
            await TryEstablishConnectionAsync(autoLogin, token);
        }
        finally
        {
            Interlocked.Exchange(ref _isConnecting, 0);
            _connectGate.Release();
        }
    }

    private async Task<PolledMessagesRental> PollPartitionMessagesRentedAsync(Identifier streamId, Identifier topicId,
        uint? partitionId, Consumer consumer, PollingStrategy pollingStrategy, uint count, bool autoCommit,
        CancellationToken token)
    {
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
            if (responseBuffer.Memory.Length == 0)
            {
                responseBuffer.Dispose();
                return EmptyPolledMessages;
            }

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

    private Task<SendMessagesResponse> SendMessagesCoreAsync(Identifier streamId, Identifier topicId,
        Partitioning partitioning, ReadOnlySpan<Message> messages, CancellationToken token)
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

        return SendConfirmedAndDisposeAsync(payloadBuffer, payloadBufferSize, token);
    }

    private async Task<SendMessagesResponse> SendConfirmedAndDisposeAsync(IMemoryOwner<byte> payloadBuffer,
        int payloadBufferSize, CancellationToken token)
    {
        try
        {
            using IMemoryOwner<byte> responseBuffer =
                await SendWithResponseAsync(payloadBuffer.Memory[..payloadBufferSize], token);
            return BinaryMapper.MapSendMessages(responseBuffer.Memory.Span);
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

    private async Task TryEstablishConnectionAsync(bool autoLogin, CancellationToken token)
    {
        var retryCount = 0;
        var redirects = 0;
        var delay = _configuration.ReconnectionSettings.InitialDelay;
        do
        {
            // The sending semaphore owns every write to _stream, so an in-flight request never observes the
            // field changing between its write and its reply.
            await _sendingSemaphore.WaitAsync(token);
            try
            {
                _stream?.Dispose();

                ResetConsensusSession();
            }
            finally
            {
                _sendingSemaphore.Release();
            }

            if (string.IsNullOrEmpty(_currentAddress))
            {
                _currentAddress = _configuration.BaseAddress;
            }

            if (!ServerAddress.TryParse(_currentAddress, out var host, out var port))
            {
                throw new InvalidBaseAddressException();
            }

            Socket? socket = null;
            try
            {
                socket = new Socket(ServerAddress.AddressFamilyOf(host), SocketType.Stream, ProtocolType.Tcp);
                socket.SendBufferSize = _configuration.SendBufferSize;
                socket.ReceiveBufferSize = _configuration.ReceiveBufferSize;

                // The protocol is request/reply, so a write is always the last one before
                // the client blocks on the answer and Nagle has nothing to coalesce it with - it only delays the
                // trailing segment of a large request until the previous one is acked.
                socket.NoDelay = true;

                await socket.ConnectAsync(host, port, token);

                _currentRemoteAddress = socket.RemoteEndPoint is IPEndPoint remote
                    ? ServerAddress.HostPort(remote.Address.ToString(), (ushort)remote.Port)
                    : string.Empty;

                socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
                socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveTime, 5);

                var connectionStream = _configuration.TlsSettings.Enabled switch
                {
                    true => await CreateSslStreamAndAuthenticate(socket, _configuration.TlsSettings),
                    false => new TcpConnectionStream(new NetworkStream(socket, true))
                };

                await _sendingSemaphore.WaitAsync(token);
                try
                {
                    _stream = connectionStream;
                }
                finally
                {
                    _sendingSemaphore.Release();
                }

                SetConnectionStateAsync(ConnectionState.Connected);
                _lastConnectionTime = DateTimeOffset.UtcNow;

                socket = null;

                // No pre-login roster read: the server auth-gates cluster metadata, so leadership settles after
                // a sign-in binds a session. A login dialed at a backup still succeeds because the server
                // forwards the register to the primary.
                if (autoLogin && _configuration.AutoLoginSettings.Enabled && !ConsumeSkipAutoLogin())
                {
                    _logger.LogInformation("Auto login enabled. Trying to login with credentials: {Username}",
                        _configuration.AutoLoginSettings.Username);
                    await LoginUserAsync(_configuration.AutoLoginSettings.Username,
                        _configuration.AutoLoginSettings.Password, token);

                    if (await RedirectAsync(token))
                    {
                        await BackoffOrThrowAsync();
                        continue;
                    }
                }

                break;
            }
            catch (Exception e)
            {
                socket?.Dispose();

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

        // A redirect restarts the loop without passing through the catch, so it spends no retry and waits for
        // nothing. Its own budget rather than the reconnection one: following the roster to the leader is how a
        // VSR connect succeeds, and it has to work with reconnection turned off.
        async Task BackoffOrThrowAsync()
        {
            if (++redirects > VsrMaxLeaderRedirects)
            {
                SetConnectionStateAsync(ConnectionState.Disconnected);
                throw new MissingLeaderException();
            }

            _logger.LogInformation("Following leader redirect {Redirect} to {Address}", redirects, _currentAddress);

            await Task.Delay(delay, token);
        }
    }

    /// <summary>
    ///     Whether this connect was triggered by a login or register request that will re-authenticate itself,
    ///     so the auto-login must sit this one out. Consumes the flag.
    /// </summary>
    private bool ConsumeSkipAutoLogin()
    {
        if (Interlocked.Exchange(ref _skipAutoLoginOnce, 0) == 0)
        {
            return false;
        }

        _logger.LogInformation("Skipping auto login for a replayed register request");

        return true;
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
        catch (Exception e) when (IsConnectionException(e) && !IsConnecting && !_disposed)
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

    private Task<IMemoryOwner<byte>> SendRawAsync(ReadOnlyMemory<byte> payload, CancellationToken token)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_state is ConnectionState.Disconnected or ConnectionState.Connecting)
        {
            throw new NotConnectedException();
        }

        return SendRawVsrAsync(payload, token);
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
