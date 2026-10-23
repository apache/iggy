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
using System.Runtime.InteropServices;
using System.Runtime.ExceptionServices;
using System.Security.Cryptography.X509Certificates;
using Apache.Iggy.Configuration;
using Apache.Iggy.Contracts;
using Apache.Iggy.Contracts.Auth;
using Apache.Iggy.Contracts.Tcp;
using Apache.Iggy.Encryption;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Headers;
using Apache.Iggy.Kinds;
using Apache.Iggy.Mappers;
using Apache.Iggy.Messages;
using Apache.Iggy.Utils;
using Apache.Iggy.Vsr;
using Microsoft.Extensions.Logging;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.IggyClient.Implementations;

/// <summary>
///     A TCP client with one logical consensus session and independently managed, multiplexed replica links.
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

    // The configurator stays settable after construction, so the endpoint the client was built for is pinned here:
    // a later assignment must not move a live replica link or the dial order.
    private readonly string _baseAddress;
    private readonly MutationLanes _mutations;
    private readonly SemaphoreSlim _connectGate = new(1, 1);
    private readonly CancellationTokenSource _clientLifetime = new();
    private CancellationTokenSource? _sessionRecoveryCancellation;
    private Task? _sessionRecovery;
    private readonly EventAggregator<ConnectionStateChangedEventArgs> _connectionEvents;
    private CancellationTokenSource? _heartbeatCancellation;
    private readonly ILogger<TcpMessageStream> _logger;
    private readonly SemaphoreSlim _sendingSemaphore;
    private VsrConnection? _connection;
    private string _currentAddress = string.Empty;

    private string _connectedAddress = string.Empty;
    private volatile bool _disposed;
    private Task? _heartbeatTask;
    private int _stateValue = (int)ConnectionState.Disconnected;

    // Every node the roster named on the last read, kept as dial candidates. A node dies together with its
    // address, and the roster is unreachable exactly when it is needed, so the client has to have remembered it
    // while the connection was still healthy. Written by the leader probe, read by the connect loop.
    private string[] _rosterAddresses = [];

    // The credentials a sign-in succeeded with, so a reconnect - on this node or, after a failover, another one -
    // can re-establish the session instead of leaving every later request unauthenticated. A caller that signs in
    // by hand is otherwise less reconnectable than one that configures auto login, which is a surprising
    // difference between two ways of doing the same thing. Cleared on sign-out and on Dispose; a server-side
    // eviction is not caller intent and leaves them in place.
    private AutoLoginSettings? _rememberedLogin;
    private bool _signInWithdrawn;

    private ConnectionState State => (ConnectionState)Volatile.Read(ref _stateValue);

    internal MutationLanes Mutations => _mutations;

    internal TcpMessageStream(IggyClientConfigurator configuration, ILoggerFactory loggerFactory)
    {
        _configuration = configuration;
        _baseAddress = configuration.BaseAddress;
        _mutations = new MutationLanes(_consensusSession, configuration, SignInRecovery);
        _logger = loggerFactory.CreateLogger<TcpMessageStream>();
        _sendingSemaphore = new SemaphoreSlim(1, 1);
        _connectionEvents = new EventAggregator<ConnectionStateChangedEventArgs>(loggerFactory);
    }

    /// <summary>
    ///     Closes the underlying connection and stops the heartbeat. The semaphores are left alone on purpose: a
    ///     heartbeat or request still in flight releases them from its finally block, and a disposed
    ///     <see cref="SemaphoreSlim" /> turns that release into an <see cref="ObjectDisposedException" /> that
    ///     replaces the real error and skips the connection drop. They own no unmanaged handle, so there is
    ///     nothing to leak. The heartbeat token source is kept undisposed for the same reason: the loop and
    ///     <see cref="StartHeartbeat" /> read its token after this method may already have run.
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _clientLifetime.Cancel();
        CancelSessionRecovery();
        StopHeartbeat();
        StopReplicaLinks();
        _mutations.Reset();
        _connection?.Dispose();
        _connection = null;
        ClearPollSession();

        // Nothing can reconnect after this, and the remembered sign-in holds a plain-string password
        // or token.
        lock (_pollRoutingGate)
        {
            _rememberedLogin = null;
            _rememberedUserId = null;
            _configuredLoginOverride = null;
        }

        SetConnectionState(ConnectionState.Disconnected);
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
    public async Task<StreamResponse?> CreateStreamAsync(string name, StreamOptions? options = null,
        CancellationToken token = default)
    {
        var message = TcpContracts.CreateStream(name, options?.ToDictionary());
        using IMemoryOwner<byte> responseBuffer
            = await SendWithResponseAsync(CommandCodes.CREATE_STREAM_CODE, message, token: token);

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
            = await SendWithResponseAsync(CommandCodes.GET_STREAM_CODE, message, token: token);

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
            = await SendWithResponseAsync(CommandCodes.GET_STREAMS_CODE, message, token: token);

        if (responseBuffer.Memory.Length == 0)
        {
            return [];
        }

        return BinaryMapper.MapStreams(responseBuffer.Memory.Span);
    }

    /// <inheritdoc />
    public async Task UpdateStreamAsync(Identifier streamId, StreamUpdate update, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(update);
        var message = TcpContracts.UpdateStream(streamId, update.ToPatch());
        await SendAckAsync(CommandCodes.UPDATE_RESOURCE_CODE, message, token);
    }

    /// <inheritdoc />
    public async Task DeleteStreamAsync(Identifier streamId, CancellationToken token = default)
    {
        var message = TcpMessageStreamHelpers.GetBytesFromIdentifier(streamId);
        await SendAckAsync(CommandCodes.DELETE_STREAM_CODE, message, token);
        _groupState.InvalidateTopicDiscovery();
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<TopicResponse>> GetTopicsAsync(Identifier streamId,
        CancellationToken token = default)
    {
        var message = TcpMessageStreamHelpers.GetBytesFromIdentifier(streamId);
        using IMemoryOwner<byte> responseBuffer
            = await SendWithResponseAsync(CommandCodes.GET_TOPICS_CODE, message, token: token);

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
            = await SendWithResponseAsync(CommandCodes.GET_TOPIC_CODE, message, token: token);

        if (responseBuffer.Memory.Length == 0)
        {
            return null;
        }

        return BinaryMapper.MapTopic(responseBuffer.Memory.Span);
    }

    /// <inheritdoc />
    public async Task<TopicResponse?> CreateTopicAsync(Identifier streamId, string name, uint partitionsCount,
        TopicOptions? options = null, CancellationToken token = default)
    {
        var message = TcpContracts.CreateTopic(streamId, name, partitionsCount,
            (options ?? new TopicOptions()).ToDictionary());

        using IMemoryOwner<byte> responseBuffer = await SendWithResponseAsync(CommandCodes.CREATE_TOPIC_CODE, message, token: token);

        if (responseBuffer.Memory.Length == 0)
        {
            return null;
        }

        return BinaryMapper.MapTopic(responseBuffer.Memory.Span);
    }

    /// <inheritdoc />
    public async Task UpdateTopicAsync(Identifier streamId, Identifier topicId, TopicUpdate update,
        CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(update);
        var message = TcpContracts.UpdateTopic(streamId, topicId, update.ToPatch());
        await SendAckAsync(CommandCodes.UPDATE_RESOURCE_CODE, message, token);
    }

    /// <inheritdoc />
    public async Task DeleteTopicAsync(Identifier streamId, Identifier topicId, CancellationToken token = default)
    {
        var message = TcpContracts.DeleteTopic(streamId, topicId);
        await SendAckAsync(CommandCodes.DELETE_TOPIC_CODE, message, token);
        _groupState.InvalidateTopicDiscovery();
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
        return PollMessagesRentedAsync(streamId, topicId, partitionId, consumer, _ => pollingStrategy, count,
            autoCommit, token);
    }

    /// <inheritdoc />
    public async Task<PolledMessages> PollMessagesAsync(Identifier streamId, Identifier topicId, uint? partitionId,
        Consumer consumer, Func<uint, PollingStrategy> pollingStrategyFor, uint count, bool autoCommit,
        CancellationToken token = default)
    {
        using var rental = await PollMessagesRentedAsync(streamId, topicId, partitionId, consumer, pollingStrategyFor,
            count, autoCommit, token);
        return BinaryMapper.MaterializeMessages(rental);
    }

    /// <inheritdoc />
    public Task<PolledMessagesRental> PollMessagesRentedAsync(Identifier streamId, Identifier topicId,
        uint? partitionId, Consumer consumer, Func<uint, PollingStrategy> pollingStrategyFor, uint count,
        bool autoCommit, CancellationToken token = default)
    {
        var generation = _consensusSession.Generation;
        ThrowIfAutoCommitWithEncryptor(autoCommit);

        // The broker routes explicit partitions only, so a group poll picks one of the member's assigned
        // partitions client-side.
        if (consumer.Type == ConsumerType.ConsumerGroup && partitionId is null)
        {
            return PollGroupMessagesRentedAsync(streamId, topicId, consumer, pollingStrategyFor, count, autoCommit,
                token, generation);
        }

        return PollPartitionMessagesRentedAsync(streamId, topicId, partitionId, consumer,
            pollingStrategyFor(partitionId ?? 0), count, autoCommit, token, generation);
    }

    /// <inheritdoc />
    public async Task StoreOffsetAsync(Consumer consumer, Identifier streamId, Identifier topicId, ulong offset,
        uint? partitionId, CancellationToken token = default)
    {
        var message = TcpContracts.UpdateOffset(streamId, topicId, consumer, offset, partitionId);
        using var response = await SendWithResponseAsync(CommandCodes.STORE_CONSUMER_OFFSET_CODE, message, token: token);
    }

    /// <inheritdoc />
    public async Task StoreOffsetAsync(Consumer consumer, Identifier streamId, Identifier topicId, ulong offset,
        uint partitionId, PartitionContext context, CancellationToken token = default)
    {
        var message = TcpContracts.UpdateOffset(streamId, topicId, consumer, offset, partitionId);
        using var response = await SendWithResponseAsync(CommandCodes.STORE_CONSUMER_OFFSET_CODE, message,
            token: token, capturedContext: context);
    }

    /// <inheritdoc />
    public async Task<OffsetResponse?> GetOffsetAsync(Consumer consumer, Identifier streamId, Identifier topicId,
        uint? partitionId, CancellationToken token = default)
    {
        var message = TcpContracts.GetOffset(streamId, topicId, consumer, partitionId ?? 0);
        using IMemoryOwner<byte> responseBuffer
            = await SendWithResponseAsync(CommandCodes.GET_CONSUMER_OFFSET_CODE, message, token: token);

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
        using var response = await SendWithResponseAsync(CommandCodes.DELETE_CONSUMER_OFFSET_CODE, message, token: token);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<ConsumerGroupResponse>> GetConsumerGroupsAsync(Identifier streamId,
        Identifier topicId,
        CancellationToken token = default)
    {
        var message = TcpContracts.GetGroups(streamId, topicId);
        using IMemoryOwner<byte> responseBuffer
            = await SendWithResponseAsync(CommandCodes.GET_CONSUMER_GROUPS_CODE, message, token: token);

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
            = await SendWithResponseAsync(CommandCodes.GET_CONSUMER_GROUP_CODE, message, token: token);

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
            = await SendWithResponseAsync(CommandCodes.CREATE_CONSUMER_GROUP_CODE, message, token: token);

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
        _groupState.InvalidateTopicDiscovery();
    }

    /// <inheritdoc />
    public async Task CreatePartitionsAsync(Identifier streamId, Identifier topicId, uint partitionsCount,
        CancellationToken token = default)
    {
        var message = TcpContracts.CreatePartitions(streamId, topicId, partitionsCount);
        await SendAckAsync(CommandCodes.CREATE_PARTITIONS_CODE, message, token);
        _groupState.InvalidateTopicDiscovery();
    }

    /// <inheritdoc />
    public async Task DeleteSegmentsAsync(Identifier streamId, Identifier topicId, uint partitionId,
        uint segmentsCount, CancellationToken token = default)
    {
        var message = TcpContracts.DeleteSegments(streamId, topicId, partitionId, segmentsCount);
        await SendAckAsync(CommandCodes.DELETE_SEGMENTS_CODE, message, token);
    }

    /// <inheritdoc />
    public async Task<ClientResponse?> GetMeAsync(CancellationToken token = default)
    {
        var message = Array.Empty<byte>();
        using IMemoryOwner<byte> responseBuffer = await SendWithResponseAsync(CommandCodes.GET_ME_CODE, message, token: token);

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
            = await SendWithResponseAsync(CommandCodes.GET_STATS_CODE, message, token: token);

        if (responseBuffer.Memory.Length == 0)
        {
            return null;
        }

        return BinaryMapper.MapStats(responseBuffer.Memory.Span);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<OptionSpec>> DescribeOptionsAsync(OptionsScope scope,
        CancellationToken token = default)
    {
        var message = new[] { (byte)scope };
        using IMemoryOwner<byte> responseBuffer
            = await SendWithResponseAsync(CommandCodes.DESCRIBE_OPTIONS_CODE, message, token: token);

        if (responseBuffer.Memory.Length == 0)
        {
            return [];
        }

        return BinaryMapper.MapOptionSpecs(responseBuffer.Memory.Span);
    }

    /// <inheritdoc />
    public async Task<ClusterMetadata?> GetClusterMetadataAsync(CancellationToken token = default)
    {
        var message = Array.Empty<byte>();
        using IMemoryOwner<byte> responseBuffer
            = await SendWithResponseAsync(CommandCodes.GET_CLUSTER_METADATA_CODE, message, token: token);

        if (responseBuffer.Memory.Length == 0)
        {
            return null;
        }

        var metadata = BinaryMapper.MapClusterMetadata(responseBuffer.Memory.Span);
        RememberRoster(metadata);
        return metadata;
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
        using IMemoryOwner<byte> result = await SendWithResponseAsync(CommandCodes.GET_SNAPSHOT_CODE, message, token: token);

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

        using IMemoryOwner<byte> result = await SendWithResponseAsync((int)code, payload, token: token);

        return result.Memory.Length == 0 ? [] : result.Memory.Span.ToArray();
    }

    /// <inheritdoc />
    public Task ConnectAsync(CancellationToken token = default)
    {
        if (_configuration.ReconnectionSettings.Enabled && !_configuration.AutoLoginSettings.Enabled)
        {
            _logger.LogWarning(
                "Reconnection is enabled without auto login: a lost session can only be restored once the client has signed in at least once");
        }

        return ConnectAsync(true, token);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<ClientResponse>> GetClientsAsync(CancellationToken token = default)
    {
        var message = Array.Empty<byte>();
        using IMemoryOwner<byte> responseBuffer
            = await SendWithResponseAsync(CommandCodes.GET_CLIENTS_CODE, message, token: token);

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
            = await SendWithResponseAsync(CommandCodes.GET_CLIENT_CODE, message, token: token);

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
            = await SendWithResponseAsync(CommandCodes.GET_USER_CODE, message, token: token);

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
            = await SendWithResponseAsync(CommandCodes.GET_USERS_CODE, message, token: token);

        if (responseBuffer.Memory.Length == 0)
        {
            return [];
        }

        return BinaryMapper.MapUsers(responseBuffer.Memory.Span);
    }

    /// <inheritdoc />
    public async Task<UserResponse?> CreateUserAsync(string userName, string password, UserStatus status,
        Permissions? permissions = null, UserOptions? options = null, CancellationToken token = default)
    {
        var message = TcpContracts.CreateUser(userName, password, status, permissions, options?.ToDictionary());
        using IMemoryOwner<byte> responseBuffer
            = await SendWithResponseAsync(CommandCodes.CREATE_USER_CODE, message, token: token);

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
    public async Task UpdateUserAsync(Identifier userId, UserUpdate update, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(update);
        var message = TcpContracts.UpdateUser(userId, update.ToPatch());
        await SendAckAsync(CommandCodes.UPDATE_RESOURCE_CODE, message, token);
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
    public Task<AuthResponse?> LoginUserAsync(string userName, string password, CancellationToken token = default)
    {
        return LoginRegisterAsync(new AutoLoginSettings { Enabled = true, Username = userName, Password = password },
            false, token);
    }

    /// <inheritdoc />
    public async Task LogoutUserAsync(CancellationToken token = default)
    {
        var owner = BeginExplicitSignIn();
        var acquired = false;
        try
        {
            await _connectGate.WaitAsync(token);
            acquired = true;
            EnsureSignInOwner(owner);
            await LogoutCoreAsync(token, owner);
        }
        finally
        {
            await ResetConsensusSessionAsync(owner: owner);
            if (acquired)
            {
                _connectGate.Release();
            }
        }
    }

    private async Task LogoutCoreAsync(CancellationToken token, ulong owner)
    {
        try
        {
            using var response = await SendWithResponseAsync(CommandCodes.LOGOUT_USER_CODE,
                ReadOnlyMemory<byte>.Empty, token, startedUnder: owner);
        }
        finally
        {
            await ResetConsensusSessionAsync(owner: owner, continues: true);
        }
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<PersonalAccessTokenResponse>> GetPersonalAccessTokensAsync(
        CancellationToken token = default)
    {
        var message = Array.Empty<byte>();
        using IMemoryOwner<byte> responseBuffer
            = await SendWithResponseAsync(CommandCodes.GET_PERSONAL_ACCESS_TOKENS_CODE, message, token: token);

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
            = await SendWithResponseAsync(CommandCodes.CREATE_PERSONAL_ACCESS_TOKEN_CODE, message, token: token);

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
    public Task<AuthResponse?> LoginWithPersonalAccessTokenAsync(string token, CancellationToken ct = default)
    {
        return LoginRegisterAsync(new AutoLoginSettings { Enabled = true, PersonalAccessToken = token }, true, ct);
    }

    private async Task ConnectAsync(bool autoLogin, CancellationToken token)
    {
        var owner = _consensusSession.Generation;
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(token, _clientLifetime.Token);
        token = cancellation.Token;
        await _connectGate.WaitAsync(token);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (ReadyReplicaLinks().Length > 0)
            {
                return;
            }
            StopReplicaLinks();
            SetConnectionState(ConnectionState.Connecting);
            await TryEstablishConnectionAsync(autoLogin, token, owner);
            if (_disposed)
            {
                await DropConnectionAsync();
                throw new ObjectDisposedException(nameof(TcpMessageStream));
            }
            StartHeartbeat();
        }
        finally
        {
            _connectGate.Release();
        }
    }

    private void StartHeartbeat()
    {
        lock (_pollRoutingGate)
        {
            if (_disposed || !_consensusSession.IsBound || _heartbeatTask is { IsCompleted: false })
            {
                return;
            }
            _heartbeatCancellation = new CancellationTokenSource();
            _heartbeatTask = RunHeartbeatAsync(_configuration.HeartbeatInterval, _heartbeatCancellation.Token);
        }
    }

    private void StopHeartbeat()
    {
        lock (_pollRoutingGate)
        {
            _heartbeatCancellation?.Cancel();
            _heartbeatCancellation = null;
            _heartbeatTask = null;
        }
    }

    private async Task RunHeartbeatAsync(TimeSpan interval, CancellationToken token)
    {
        // The group refresh of a tick gets one tick in all, so a coordinator that does not answer delays the next
        // link restart by one tick, whatever the number of groups.
        var refreshBudgetMs = (long)Math.Min(VsrRequestTimeoutMs, interval.TotalMilliseconds);
        try
        {
            using var timer = new PeriodicTimer(interval);
            while (await timer.WaitForNextTickAsync(token))
            {
                RestartExhaustedReplicaLinks();
                PingReadyReplicas();
                // A disconnected client has no link a group sync could use, so the sync would only wait out its
                // timeout.
                if (State == ConnectionState.Authenticated)
                {
                    await RefreshGroupAssignmentsAsync(token, Environment.TickCount64 + refreshBudgetMs);
                }
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
        }
        catch (Exception error)
        {
            _logger.LogError(error, "Heartbeat stopped");
        }
    }

    private async Task<PolledMessagesRental> PollPartitionMessagesRentedAsync(Identifier streamId, Identifier topicId,
        uint? partitionId, Consumer consumer, PollingStrategy pollingStrategy, uint count, bool autoCommit,
        CancellationToken token, ulong generation)
    {
        var messageBufferSize = CalculateMessageBufferSize(streamId, topicId, consumer);
        var payload = ArrayPool<byte>.Shared.Rent(messageBufferSize);
        IMemoryOwner<byte>? responseBuffer = null;

        try
        {
            TcpContracts.GetMessages(payload.AsSpan(0, messageBufferSize), consumer, streamId,
                topicId, pollingStrategy, count, autoCommit, partitionId ?? 0);

            var body = payload.AsMemory(0, messageBufferSize);
            responseBuffer = await SendWithResponseAsync(CommandCodes.POLL_MESSAGES_CODE, body,
                token: token, capturedContext: pollingStrategy.Context, startedUnder: generation);
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
        Partitioning partitioning, ReadOnlySpan<Message> messages, CancellationToken token, long? deadline = null,
        ulong? startedUnder = null)
    {
        var encryptor = _configuration.MessageEncryptor;

        // With an encryptor this is an upper bound; the fill reports the size actually written and only that
        // prefix is sent.
        var metadataLength = 2 + streamId.Length + 2 + topicId.Length
                             + 2 + partitioning.Length + 4 + 4;
        var maxMessageBufferSize = TcpMessageStreamHelpers.CalculateMessageBytesCount(messages, encryptor)
                                   + metadataLength;

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

        return SendConfirmedAndDisposeAsync(payloadBuffer, bodySize, token, deadline, startedUnder);
    }

    private async Task<SendMessagesResponse> SendConfirmedAndDisposeAsync(IMemoryOwner<byte> payloadBuffer,
        int bodySize, CancellationToken token, long? deadline, ulong? startedUnder)
    {
        try
        {
            using IMemoryOwner<byte> responseBuffer = await SendWithResponseAsync(CommandCodes.SEND_MESSAGES_CODE,
                payloadBuffer.Memory[..bodySize], token: token, deadline: deadline, startedUnder: startedUnder);
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

    private async Task TryEstablishConnectionAsync(bool autoLogin, CancellationToken token, ulong? startedUnder = null,
        bool continues = false)
    {
        var owner = startedUnder ?? _consensusSession.Generation;
        var attempts = 0;
        var candidates = DialCandidates();
        while (true)
        {
            Exception? lastError = null;
            Exception? configurationError = null;
            foreach (var endpoint in candidates)
            {
                VsrConnection connection;
                try
                {
                    connection = await DialReplicaAsync(endpoint, token);
                }
                catch (Exception error) when (!token.IsCancellationRequested && !_disposed)
                {
                    lastError = error;
                    if (IsConnectionConfigurationFault(error))
                    {
                        configurationError = error;
                    }
                    continue;
                }
                lock (_pollRoutingGate)
                {
                    try
                    {
                        EnsureSignInOwner(owner);
                    }
                    catch
                    {
                        connection.Dispose();
                        throw;
                    }
                    _connection = connection;
                    _connectedAddress = endpoint;
                    _currentAddress = endpoint;
                    SetConnectionState(ConnectionState.Connected);
                }
                try
                {
                    if (autoLogin && SignInSettings() is { } settings)
                    {
                        if (await ResumeSessionAsync(token, owner))
                        {
                            var roster = await ReadClusterMetadataOnReadyLinkAsync(token);
                            if (roster is not null)
                            {
                                RememberRoster(roster);
                            }
                        }
                        else
                        {
                            await AutoLoginAsync(settings, token, owner, continues);
                        }
                    }
                    return;
                }
                catch
                {
                    await DropConnectionAsync();
                    SetConnectionState(ConnectionState.Disconnected);
                    throw;
                }
            }
            if (configurationError is not null)
            {
                throw configurationError;
            }
            if (!_configuration.ReconnectionSettings.Enabled
                || _configuration.ReconnectionSettings.MaxRetries > 0 && attempts >= _configuration.ReconnectionSettings.MaxRetries)
            {
                SetConnectionState(ConnectionState.Disconnected);
                throw lastError ?? new NotConnectedException();
            }
            await Task.Delay(ReconnectDelay(attempts++), token);
        }
    }

    private async Task DropConnectionAsync()
    {
        await _sendingSemaphore.WaitAsync(CancellationToken.None);
        try
        {
            _connection?.Dispose();
            _connection = null;
        }
        finally
        {
            _sendingSemaphore.Release();
        }
    }

    private string[] DialCandidates()
    {
        return DialCandidates(_currentAddress, _baseAddress, _rosterAddresses);
    }

    /// <summary>
    ///     The endpoints one connect dials, likeliest first: where the client currently is, the address it was
    ///     configured with, then the roster it learned while connected. Duplicates are dropped, so an endpoint the
    ///     roster merely spells differently does not earn a second attempt.
    /// </summary>
    internal static string[] DialCandidates(string currentAddress, string baseAddress, string[] rosterAddresses)
    {
        var candidates = new List<string>();
        if (!string.IsNullOrEmpty(currentAddress))
        {
            candidates.Add(currentAddress);
        }

        foreach (var endpoint in rosterAddresses.Prepend(baseAddress))
        {
            if (!string.IsNullOrEmpty(endpoint) &&
                !candidates.Exists(known => ServerAddress.IsSame(known, endpoint)))
            {
                candidates.Add(endpoint);
            }
        }

        return candidates.ToArray();
    }

    private async Task AutoLoginAsync(AutoLoginSettings settings, CancellationToken token, ulong owner,
        bool continues = false)
    {
        var personalAccessToken = !string.IsNullOrEmpty(settings.PersonalAccessToken);
        var code = personalAccessToken ? CommandCodes.LOGIN_REGISTER_WITH_PAT_CODE : CommandCodes.LOGIN_REGISTER_CODE;
        var message = SerializeSignIn(settings, personalAccessToken);
        await LoginRegisterCoreAsync(code, message, settings, token, owner, continues);
    }

    private static byte[] SerializeSignIn(AutoLoginSettings settings, bool personalAccessToken)
    {
        var secret = LoginRegister.CreateBindSecret();
        return personalAccessToken
            ? LoginRegister.SerializeWithPersonalAccessToken(settings.PersonalAccessToken, secret)
            : LoginRegister.Serialize(settings.Username, settings.Password, secret);
    }

    /// <summary>Whether the server definitively refused the credential supplied to a register.</summary>
    private static bool IsSignInRefusal(Exception error) => error is IggyInvalidStatusCodeException
    {
        FromServer: true,
        StatusCode: VsrError.UNAUTHENTICATED or VsrError.INVALID_CREDENTIALS or VsrError.INVALID_USERNAME
            or VsrError.INVALID_PASSWORD or VsrError.USER_INACTIVE or VsrError.INVALID_PERSONAL_ACCESS_TOKEN
            or VsrError.PERSONAL_ACCESS_TOKEN_EXPIRED or VsrError.INVALID_FORMAT
    };

    private AutoLoginSettings? SignInSettings()
    {
        lock (_pollRoutingGate)
        {
            return _signInWithdrawn ? null : _rememberedLogin
                ?? (_configuration.AutoLoginSettings.Enabled
                    ? _configuredLoginOverride ?? _configuration.AutoLoginSettings : null);
        }
    }

    private async Task<Stream> CreateSslStreamAndAuthenticate(Socket socket, TlsSettings tlsSettings,
        CancellationToken token)
    {
        ValidateCertificatePath(tlsSettings.CertificatePath);

        var customCaStore = new X509Certificate2Collection();
        customCaStore.ImportFromPemFile(tlsSettings.CertificatePath);
        var stream = new NetworkStream(socket, true);
        var sslStream = new SslStream(stream, false, (sender, certificate, chain, errors) =>
            RemoteCertificateValidationCallback(sender, certificate, chain, errors, customCaStore));
        try
        {
            // The token carries the dial bound when other endpoints are queued behind this one: a peer that
            // accepts TCP and never answers the ClientHello has no deadline of its own here either.
            await sslStream.AuthenticateAsClientAsync(
                new SslClientAuthenticationOptions { TargetHost = tlsSettings.Hostname }, token);
        }
        catch
        {
            // A handshake that failed leaves the stream owning the socket, and the sweep moves on to the
            // next endpoint: undisposed, both leak for as long as the client lives.
            await sslStream.DisposeAsync();
            throw;
        }

        return sslStream;
    }

    private static bool IsConnectionConfigurationFault(Exception error)
    {
        // Local configuration faults cannot heal on retry. Peer handshake failures still allow fallback.
        return error is InvalidBaseAddressException or InvalidCertificatePathException;
    }

    private async Task SendAckAsync(int code, ReadOnlyMemory<byte> body, CancellationToken token)
    {
        using IMemoryOwner<byte> _ = await SendWithResponseAsync(code, body, token: token);
    }

    internal async Task<IMemoryOwner<byte>> SendWithResponseAsync(int code, ReadOnlyMemory<byte> body,
        CancellationToken token = default, PartitionContext? capturedContext = null, long? deadline = null,
        ulong? startedUnder = null)
    {
        TimeSpan? remaining = deadline is { } end ? TimeSpan.FromMilliseconds(end - Environment.TickCount64) : null;
        if (remaining <= TimeSpan.Zero)
        {
            token.ThrowIfCancellationRequested();
            throw PollNotAccepted();
        }
        if (code is CommandCodes.SEND_MESSAGES_CODE or CommandCodes.POLL_MESSAGES_CODE
            or CommandCodes.GET_CONSUMER_OFFSET_CODE or CommandCodes.STORE_CONSUMER_OFFSET_CODE
            or CommandCodes.DELETE_CONSUMER_OFFSET_CODE)
        {
            return await SendRoutedAsync(code, PartitionTarget(code, body.Span), body, capturedContext, token, remaining,
                startedUnder);
        }
        if (VsrOperations.ForCode(code) is not (VsrOperation.Register or VsrOperation.Logout))
        {
            return await RunClientOperationAsync(code, body, capturedContext, null, token, timeout: remaining,
                startedUnder: startedUnder);
        }
        return await SendSessionControlAsync(code, body, token, startedUnder);
    }

    private async Task<IMemoryOwner<byte>> SendSessionControlAsync(int code, ReadOnlyMemory<byte> body,
        CancellationToken token, ulong? owner)
    {
        var deadline = Environment.TickCount64 + VsrRequestTimeoutMs;
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(token);
        cancellation.CancelAfter(VsrRequestTimeoutMs);
        await _sendingSemaphore.WaitAsync(cancellation.Token);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            SessionFrame frame;
            lock (_pollRoutingGate)
            {
                if (owner is { } generation)
                {
                    EnsureSignInOwner(generation);
                }
                frame = _consensusSession.Resolve(VsrOperations.ForCode(code));
            }
            var connection = _connection is { IsReady: true } bootstrap
                ? bootstrap : ReadyReplicaLinks().FirstOrDefault() ?? throw new NotConnectedException();
            // The attempt keeps the deadline itself and returns the refusal it holds at the deadline. A timer token
            // would end its retry pause with a cancellation, which reads as an unknown outcome.
            var attempt = await connection.SendAttemptAsync(code, body, deadline, deadline, HasSensitiveReply(code),
                token, retainedFrame: frame);
            if (attempt.Error is null)
            {
                return attempt.Response!;
            }
            if (VsrOperations.ForCode(code) == VsrOperation.Register
                && attempt.Error is VsrSessionEvictedException { InnerException: { } rejection })
            {
                ExceptionDispatchInfo.Throw(rejection);
            }
            if (attempt.RequestStarted && attempt.Error is not IggyInvalidStatusCodeException
                { FromServer: true, StatusCode: not VsrError.TRANSIENT_NOT_COMMITTED })
            {
                throw new VsrRequestOutcomeUnknownException(attempt.Error);
            }
            ExceptionDispatchInfo.Throw(CallerError(attempt.Error));
            throw new InvalidOperationException();
        }
        finally
        {
            _sendingSemaphore.Release();
        }
    }

    private static int CalculateMessageBufferSize(Identifier streamId, Identifier topicId, Consumer consumer)
    {
        // Original: 14 + 5 + 2 + streamId.Length + 2 + topicId.Length + 2 + consumer.Id.Length
        // Added 1 byte for partition flag
        return 15 + 5 + 2 + streamId.Length + 2 + topicId.Length + 2 + consumer.ConsumerId.Length;
    }

    /// <summary>
    ///     Link and session owners transition independently. An atomic swap preserves the previous state
    ///     and prevents duplicate notifications when several links change together.
    /// </summary>
    /// <param name="newState">The new connection state</param>
    private void SetConnectionState(ConnectionState newState)
    {
        var previousState = (ConnectionState)Interlocked.Exchange(ref _stateValue, (int)newState);
        if (previousState == newState)
        {
            return;
        }

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
        SslPolicyErrors sslPolicyErrors, X509Certificate2Collection customCaStore)
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

        if (customCaStore.Any(ca => ca.Thumbprint == serverCert.Thumbprint))
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
        foreach (var ca in customCaStore)
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
