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
using System.Runtime.InteropServices;
using System.Security.Cryptography.X509Certificates;
using Apache.Iggy.Configuration;
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
    private VsrConnection? _connection;
    private string _currentAddress = string.Empty;
    private X509Certificate2Collection _customCaStore = [];
    private volatile bool _disposed;
    private int _isConnecting;
    private DateTimeOffset _lastConnectionTime;
    private volatile ConnectionState _state = ConnectionState.Disconnected;

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
        _connection?.Close();
        _connection?.Dispose();

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
        using IMemoryOwner<byte> responseBuffer
            = await SendWithResponseAsync(CommandCodes.CREATE_STREAM_CODE, message, token);

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
        using IMemoryOwner<byte> responseBuffer
            = await SendWithResponseAsync(CommandCodes.GET_STREAM_CODE, message, token);

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
        using IMemoryOwner<byte> responseBuffer
            = await SendWithResponseAsync(CommandCodes.GET_STREAMS_CODE, message, token);

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
        await SendAckAsync(CommandCodes.UPDATE_STREAM_CODE, message, token);
    }

    /// <inheritdoc />
    public async Task PurgeStreamAsync(Identifier streamId, CancellationToken token = default)
    {
        var message = TcpMessageStreamHelpers.GetBytesFromIdentifier(streamId);
        await SendAckAsync(CommandCodes.PURGE_STREAM_CODE, message, token);
    }

    /// <inheritdoc />
    public async Task DeleteStreamAsync(Identifier streamId, CancellationToken token = default)
    {
        var message = TcpMessageStreamHelpers.GetBytesFromIdentifier(streamId);
        await SendAckAsync(CommandCodes.DELETE_STREAM_CODE, message, token);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<TopicResponse>> GetTopicsAsync(Identifier streamId,
        CancellationToken token = default)
    {
        var message = TcpMessageStreamHelpers.GetBytesFromIdentifier(streamId);
        using IMemoryOwner<byte> responseBuffer
            = await SendWithResponseAsync(CommandCodes.GET_TOPICS_CODE, message, token);

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
        using IMemoryOwner<byte> responseBuffer
            = await SendWithResponseAsync(CommandCodes.GET_TOPIC_CODE, message, token);

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
        using IMemoryOwner<byte> responseBuffer
            = await SendWithResponseAsync(CommandCodes.CREATE_TOPIC_CODE, message, token);

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
        await SendAckAsync(CommandCodes.UPDATE_TOPIC_CODE, message, token);
    }

    /// <inheritdoc />
    public async Task DeleteTopicAsync(Identifier streamId, Identifier topicId, CancellationToken token = default)
    {
        var message = TcpContracts.DeleteTopic(streamId, topicId);
        await SendAckAsync(CommandCodes.DELETE_TOPIC_CODE, message, token);
        _groupState.InvalidatePartitionCount(new TopicKey(streamId, topicId));
    }

    /// <inheritdoc />
    public async Task PurgeTopicAsync(Identifier streamId, Identifier topicId, CancellationToken token = default)
    {
        var message = TcpContracts.PurgeTopic(streamId, topicId);
        await SendAckAsync(CommandCodes.PURGE_TOPIC_CODE, message, token);
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
        var ns = ConsumerOffsetNamespace(streamId, topicId, partitionId);
        var message = TcpContracts.UpdateOffset(streamId, topicId, consumer, offset, partitionId);
        await SendAckAsync(CommandCodes.STORE_CONSUMER_OFFSET_CODE, ns, message, token);
    }

    /// <inheritdoc />
    public async Task<OffsetResponse?> GetOffsetAsync(Consumer consumer, Identifier streamId, Identifier topicId,
        uint? partitionId, CancellationToken token = default)
    {
        var message = TcpContracts.GetOffset(streamId, topicId, consumer, partitionId);
        using IMemoryOwner<byte> responseBuffer
            = await SendWithResponseAsync(CommandCodes.GET_CONSUMER_OFFSET_CODE, message, token);

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
        var ns = ConsumerOffsetNamespace(streamId, topicId, partitionId);
        var message = TcpContracts.DeleteOffset(streamId, topicId, consumer, partitionId);
        await SendAckAsync(CommandCodes.DELETE_CONSUMER_OFFSET_CODE, ns, message, token);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<ConsumerGroupResponse>> GetConsumerGroupsAsync(Identifier streamId,
        Identifier topicId,
        CancellationToken token = default)
    {
        var message = TcpContracts.GetGroups(streamId, topicId);
        using IMemoryOwner<byte> responseBuffer
            = await SendWithResponseAsync(CommandCodes.GET_CONSUMER_GROUPS_CODE, message, token);

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
        using IMemoryOwner<byte> responseBuffer
            = await SendWithResponseAsync(CommandCodes.GET_CONSUMER_GROUP_CODE, message, token);

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
        using IMemoryOwner<byte> responseBuffer
            = await SendWithResponseAsync(CommandCodes.CREATE_CONSUMER_GROUP_CODE, message, token);

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
        await SendAckAsync(CommandCodes.DELETE_CONSUMER_GROUP_CODE, message, token);
        _groupState.DeregisterGroup(new GroupKey(streamId, topicId, groupId));
    }

    /// <inheritdoc />
    public async Task JoinConsumerGroupAsync(Identifier streamId, Identifier topicId, Identifier groupId,
        CancellationToken token = default)
    {
        var message = TcpContracts.JoinGroup(streamId, topicId, groupId);
        await SendAckAsync(CommandCodes.JOIN_CONSUMER_GROUP_CODE, message, token);

        // A join rebalances the group, so whatever this client holds for it is a generation behind and every
        // poll under it would be fenced until the first re-sync.
        _groupState.InvalidateAssignment(new GroupKey(streamId, topicId, groupId));
    }

    /// <inheritdoc />
    public async Task LeaveConsumerGroupAsync(Identifier streamId, Identifier topicId, Identifier groupId,
        CancellationToken token = default)
    {
        var message = TcpContracts.LeaveGroup(streamId, topicId, groupId);
        await SendAckAsync(CommandCodes.LEAVE_CONSUMER_GROUP_CODE, message, token);
        _groupState.DeregisterGroup(new GroupKey(streamId, topicId, groupId));
    }

    /// <inheritdoc />
    public async Task DeletePartitionsAsync(Identifier streamId, Identifier topicId, uint partitionsCount,
        CancellationToken token = default)
    {
        var message = TcpContracts.DeletePartitions(streamId, topicId, partitionsCount);
        await SendAckAsync(CommandCodes.DELETE_PARTITIONS_CODE, message, token);
        _groupState.InvalidatePartitionCount(new TopicKey(streamId, topicId));
    }

    /// <inheritdoc />
    public async Task CreatePartitionsAsync(Identifier streamId, Identifier topicId, uint partitionsCount,
        CancellationToken token = default)
    {
        var message = TcpContracts.CreatePartitions(streamId, topicId, partitionsCount);
        await SendAckAsync(CommandCodes.CREATE_PARTITIONS_CODE, message, token);
        _groupState.InvalidatePartitionCount(new TopicKey(streamId, topicId));
    }

    /// <inheritdoc />
    public async Task DeleteSegmentsAsync(Identifier streamId, Identifier topicId, uint partitionId,
        uint segmentsCount, CancellationToken token = default)
    {
        var ns = VsrNamespace.ForPartition(streamId, topicId, partitionId);
        var message = TcpContracts.DeleteSegments(streamId, topicId, partitionId, segmentsCount);
        await SendAckAsync(CommandCodes.DELETE_SEGMENTS_CODE, ns, message, token);
    }

    /// <inheritdoc />
    public async Task<ClientResponse?> GetMeAsync(CancellationToken token = default)
    {
        var message = Array.Empty<byte>();
        using IMemoryOwner<byte> responseBuffer = await SendWithResponseAsync(CommandCodes.GET_ME_CODE, message, token);

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
        using IMemoryOwner<byte> responseBuffer
            = await SendWithResponseAsync(CommandCodes.GET_STATS_CODE, message, token);

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
        using IMemoryOwner<byte> responseBuffer
            = await SendWithResponseAsync(CommandCodes.GET_CLUSTER_METADATA_CODE, message, token);

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
        await SendAckAsync(CommandCodes.PING_CODE, message, token);

        await RefreshGroupAssignmentsAsync(token);
    }

    /// <inheritdoc />
    public async Task<byte[]> GetSnapshotAsync(SnapshotCompression compression,
        IList<SystemSnapshotType> snapshotTypes, CancellationToken token = default)
    {
        var message = TcpContracts.GetSnapshot(compression, snapshotTypes);
        using IMemoryOwner<byte> result = await SendWithResponseAsync(CommandCodes.GET_SNAPSHOT_CODE, message, token);

        return result.Memory.Span.ToArray();
    }

    /// <inheritdoc />
    public async Task<byte[]> SendBinaryRequestAsync(uint code, byte[] payload, CancellationToken token = default)
    {
        if (SessionControlCodes.Contains(code))
        {
            throw VsrError.Exception(VsrError.INVALID_COMMAND,
                $"Command {code} cannot be sent as a raw binary request.");
        }

        var ns = VsrNamespace.ForRequest((int)code, payload, VsrOperations.ForCode((int)code));
        using IMemoryOwner<byte> result = await SendWithResponseAsync((int)code, ns, payload, token);

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
        using IMemoryOwner<byte> responseBuffer
            = await SendWithResponseAsync(CommandCodes.GET_CLIENTS_CODE, message, token);

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
        using IMemoryOwner<byte> responseBuffer
            = await SendWithResponseAsync(CommandCodes.GET_CLIENT_CODE, message, token);

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
        using IMemoryOwner<byte> responseBuffer
            = await SendWithResponseAsync(CommandCodes.GET_USER_CODE, message, token);

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
        using IMemoryOwner<byte> responseBuffer
            = await SendWithResponseAsync(CommandCodes.GET_USERS_CODE, message, token);

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
        using IMemoryOwner<byte> responseBuffer
            = await SendWithResponseAsync(CommandCodes.CREATE_USER_CODE, message, token);

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
        await SendAckAsync(CommandCodes.DELETE_USER_CODE, message, token);
    }

    /// <inheritdoc />
    public async Task UpdateUserAsync(Identifier userId, string? userName = null, UserStatus? status = null,
        CancellationToken token = default)
    {
        var message = TcpContracts.UpdateUser(userId, userName, status);
        await SendAckAsync(CommandCodes.UPDATE_USER_CODE, message, token);
    }

    /// <inheritdoc />
    public async Task UpdatePermissionsAsync(Identifier userId, Permissions? permissions = null,
        CancellationToken token = default)
    {
        var message = TcpContracts.UpdatePermissions(userId, permissions);
        await SendAckAsync(CommandCodes.UPDATE_PERMISSIONS_CODE, message, token);
    }

    /// <inheritdoc />
    public async Task ChangePasswordAsync(Identifier userId, string currentPassword, string newPassword,
        CancellationToken token = default)
    {
        var message = TcpContracts.ChangePassword(userId, currentPassword, newPassword);
        await SendAckAsync(CommandCodes.CHANGE_PASSWORD_CODE, message, token);
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
        try
        {
            await SendAckAsync(CommandCodes.LOGOUT_USER_CODE, ReadOnlyMemory<byte>.Empty, token);
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
        using IMemoryOwner<byte> responseBuffer
            = await SendWithResponseAsync(CommandCodes.GET_PERSONAL_ACCESS_TOKENS_CODE, message, token);

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
        using IMemoryOwner<byte> responseBuffer
            = await SendWithResponseAsync(CommandCodes.CREATE_PERSONAL_ACCESS_TOKEN_CODE, message, token);

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
        await SendAckAsync(CommandCodes.DELETE_PERSONAL_ACCESS_TOKEN_CODE, message, token);
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
        var payload = ArrayPool<byte>.Shared.Rent(messageBufferSize);
        IMemoryOwner<byte>? responseBuffer = null;

        try
        {
            TcpContracts.GetMessages(payload.AsSpan(0, messageBufferSize), consumer, streamId,
                topicId, pollingStrategy, count, autoCommit, partitionId);

            responseBuffer = await SendWithResponseAsync(CommandCodes.POLL_MESSAGES_CODE,
                payload.AsMemory(0, messageBufferSize), token);
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

        var ns = SendMessagesNamespace(streamId, topicId, partitioning);
        IMemoryOwner<byte> payloadBuffer = MemoryPool<byte>.Shared.Rent(maxMessageBufferSize);
        int bodySize;
        try
        {
            bodySize = TcpContracts.CreateMessage(payloadBuffer.Memory.Span[..maxMessageBufferSize], streamId,
                topicId, partitioning, messages, encryptor);
        }
        catch
        {
            payloadBuffer.Dispose();
            throw;
        }

        return SendConfirmedAndDisposeAsync(ns, payloadBuffer, bodySize, token);
    }

    private async Task<SendMessagesResponse> SendConfirmedAndDisposeAsync(ulong ns, IMemoryOwner<byte> payloadBuffer,
        int bodySize, CancellationToken token)
    {
        try
        {
            using IMemoryOwner<byte> responseBuffer = await SendWithResponseAsync(CommandCodes.SEND_MESSAGES_CODE,
                ns, payloadBuffer.Memory[..bodySize], token);
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

    /// <summary>
    ///     The routing namespace for a send: callers resolve balanced and message-key partitioning to an
    ///     explicit partition before framing, so anything else reaching this point is a bug surfaced as the
    ///     same error the server-side router would raise.
    /// </summary>
    private static ulong SendMessagesNamespace(Identifier streamId, Identifier topicId, Partitioning partitioning)
    {
        if (partitioning.Kind != Enums.Partitioning.PartitionId)
        {
            throw VsrError.Exception(VsrError.FEATURE_UNAVAILABLE,
                "Under VSR the partition must be resolved by the client; balanced and message-key partitioning cannot be sent on the wire.");
        }

        if (partitioning.Value.Length < 4)
        {
            throw VsrError.Exception(VsrError.INVALID_COMMAND,
                $"Partition id is {partitioning.Value.Length} bytes, expected 4.");
        }

        return VsrNamespace.ForPartition(streamId, topicId,
            BinaryPrimitives.ReadUInt32LittleEndian(partitioning.Value));
    }

    /// <summary>
    ///     The routing namespace for a consumer-offset write. The broker routes explicit partitions only, so a
    ///     missing partition id fails client-side before a request id is consumed.
    /// </summary>
    private static ulong ConsumerOffsetNamespace(Identifier streamId, Identifier topicId, uint? partitionId)
    {
        if (partitionId is null)
        {
            throw VsrError.Exception(VsrError.INVALID_IDENTIFIER,
                "Under VSR a consumer-offset request must carry an explicit partition id.");
        }

        return VsrNamespace.ForPartition(streamId, topicId, partitionId.Value);
    }

    private async Task TryEstablishConnectionAsync(bool autoLogin, CancellationToken token)
    {
        var retryCount = 0;
        var redirects = 0;
        var delay = _configuration.ReconnectionSettings.InitialDelay;
        do
        {
            // The sending semaphore owns every write to _connection, so an in-flight request never observes
            // the field changing between its write and its reply.
            await _sendingSemaphore.WaitAsync(token);
            try
            {
                _connection?.Dispose();

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

                socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
                socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveTime, 5);

                var connectionStream = _configuration.TlsSettings.Enabled
                    ? await CreateSslStreamAndAuthenticate(socket, _configuration.TlsSettings)
                    : new NetworkStream(socket, true);

                await _sendingSemaphore.WaitAsync(token);
                try
                {
                    _connection = new VsrConnection(connectionStream, _consensusSession,
                        _configuration.MaxResponseFrameSize, VsrRequestTimeoutMs, DropVsrConnectionLocked,
                        _logger);
                }
                finally
                {
                    _sendingSemaphore.Release();
                }

                SetConnectionStateAsync(ConnectionState.Connected);
                _lastConnectionTime = DateTimeOffset.UtcNow;

                socket = null;

                if (await RedirectAsync(token))
                {
                    await BackoffOrThrowAsync();
                    continue;
                }

                if (autoLogin && _configuration.AutoLoginSettings.Enabled)
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

    private async Task<Stream> CreateSslStreamAndAuthenticate(Socket socket, TlsSettings tlsSettings)
    {
        ValidateCertificatePath(tlsSettings.CertificatePath);

        _customCaStore = new X509Certificate2Collection();
        _customCaStore.ImportFromPemFile(tlsSettings.CertificatePath);
        var stream = new NetworkStream(socket, true);
        var sslStream = new SslStream(stream, false, RemoteCertificateValidationCallback);

        await sslStream.AuthenticateAsClientAsync(tlsSettings.Hostname);

        return sslStream;
    }

    private async Task SendAckAsync(int code, ReadOnlyMemory<byte> body, CancellationToken token)
    {
        using IMemoryOwner<byte> _ = await SendWithResponseAsync(code, body, token);
    }

    private async Task SendAckAsync(int code, ulong ns, ReadOnlyMemory<byte> body, CancellationToken token)
    {
        using IMemoryOwner<byte> _ = await SendWithResponseAsync(code, ns, body, token);
    }

    private Task<IMemoryOwner<byte>> SendWithResponseAsync(int code, ReadOnlyMemory<byte> body,
        CancellationToken token)
    {
        return SendWithResponseAsync(code, 0, body, token);
    }

    private async Task<IMemoryOwner<byte>> SendWithResponseAsync(int code, ulong ns, ReadOnlyMemory<byte> body,
        CancellationToken token, bool autoLoginOnReconnect = true)
    {
        try
        {
            return await SendRawAsync(code, ns, body, token);
        }
        catch (Exception e) when (VsrConnection.IsConnectionException(e) && !IsConnecting && !_disposed)
        {
            _logger.LogWarning("Connection lost");
            if (!_configuration.ReconnectionSettings.Enabled)
            {
                _logger.LogWarning("Reconnection is disabled");
                SetConnectionStateAsync(ConnectionState.Disconnected);
                throw;
            }

            return await HandleReconnectionAsync(code, ns, body, autoLoginOnReconnect, token);
        }
    }

    private async Task<IMemoryOwner<byte>> HandleReconnectionAsync(int code, ulong ns, ReadOnlyMemory<byte> body,
        bool autoLogin, CancellationToken token)
    {
        var currentTime = DateTimeOffset.UtcNow;
        await _connectionSemaphore.WaitAsync(token);

        try
        {
            if (_state is ConnectionState.Connected or ConnectionState.Authenticated
                && _lastConnectionTime > currentTime)
            {
                _logger.LogInformation("Connection already established, sending payload");
                return await SendRawAsync(code, ns, body, token);
            }

            SetConnectionStateAsync(ConnectionState.Disconnected);
            _logger.LogInformation("Reconnecting to the server");
            await ConnectAsync(autoLogin, token);

            _logger.LogInformation("Reconnected to the server");

            await Task.Delay(_configuration.ReconnectionSettings.WaitAfterReconnect, token);

            return await SendRawAsync(code, ns, body, token);
        }
        finally
        {
            _connectionSemaphore.Release();
        }
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
}
