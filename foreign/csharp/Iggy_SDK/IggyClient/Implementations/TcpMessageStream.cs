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
    private readonly CancellationTokenSource _heartbeatCancellation = new();
    private readonly ILogger<TcpMessageStream> _logger;
    private readonly SemaphoreSlim _sendingSemaphore;
    private VsrConnection? _connection;
    private string _currentAddress = string.Empty;

    // The address the socket actually connected to, as an IP the roster can be compared against.
    // _currentAddress keeps whatever the caller configured (possibly a hostname the roster never
    // mentions), so leader comparisons made against it would move a client that is already on the
    // leader. Written only by the connect loop.
    private string _currentRemoteAddress = string.Empty;

    // The dialed address whose connect actually landed. _currentAddress moves ahead of the socket -
    // a redirect rewrites it before the reconnect - so a leader comparison against it would treat a
    // node the client never reached as "already there". Written only by the connect loop.
    private string _connectedAddress = string.Empty;
    private X509Certificate2Collection _customCaStore = [];
    private volatile bool _disposed;
    private Task? _heartbeatTask;
    private int _isConnecting;
    private DateTimeOffset _lastConnectionTime;
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

    private bool IsConnecting => Volatile.Read(ref _isConnecting) != 0;

    private ConnectionState State => (ConnectionState)Volatile.Read(ref _stateValue);

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
        _heartbeatCancellation.Cancel();
        _connection?.Dispose();
        _connection = null;

        // Nothing can reconnect after this, and the remembered sign-in holds a plain-string password
        // or token.
        _rememberedLogin = null;

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
    public async Task<StreamResponse?> CreateStreamAsync(string name, CancellationToken token = default)
    {
        var message = TcpContracts.CreateStream(name);
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
        CompressionAlgorithm compressionAlgorithm = CompressionAlgorithm.None,
        TimeSpan? messageExpiry = null, ulong maxTopicSize = 0,
        IReadOnlyDictionary<string, HeaderValue>? options = null, CancellationToken token = default)
    {
        var messageExpiryValue = DurationHelpers.ToDuration(messageExpiry);
        var message = TcpContracts.CreateTopic(streamId, name, partitionsCount, compressionAlgorithm,
            messageExpiryValue, maxTopicSize, options);

        using IMemoryOwner<byte> responseBuffer = await SendWithResponseAsync(CommandCodes.CREATE_TOPIC_CODE, message, token: token);

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
        IReadOnlyDictionary<string, HeaderValue>? options = null,
        CancellationToken token = default)
    {
        var messageExpiryValue = DurationHelpers.ToDuration(messageExpiry);
        var message = TcpContracts.UpdateTopic(streamId, topicId, name, compressionAlgorithm, maxTopicSize,
            messageExpiryValue, options);
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
        var message = TcpContracts.UpdateOffset(streamId, topicId, consumer, offset, partitionId);
        await SendAckAsync(CommandCodes.STORE_CONSUMER_OFFSET_CODE, message, token);
    }

    /// <inheritdoc />
    public async Task<OffsetResponse?> GetOffsetAsync(Consumer consumer, Identifier streamId, Identifier topicId,
        uint? partitionId, CancellationToken token = default)
    {
        var message = TcpContracts.GetOffset(streamId, topicId, consumer, partitionId);
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
        await SendAckAsync(CommandCodes.DELETE_CONSUMER_OFFSET_CODE, message, token);
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
        Permissions? permissions = null, CancellationToken token = default)
    {
        var message = TcpContracts.CreateUser(userName, password, status, permissions);
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
        if (State == ConnectionState.Disconnected)
        {
            throw new NotConnectedException();
        }

        var identity = await LoginRegisterAsync(CommandCodes.LOGIN_REGISTER_CODE,
            LoginRegister.Serialize(userName, password), token);
        _rememberedLogin = new AutoLoginSettings
        {
            Enabled = true,
            Username = userName,
            Password = password
        };

        return identity;
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

            // An explicit sign-out leaves no session to restore, so the sign-in this client remembered
            // does not outlive it. Credentials configured as AutoLoginSettings are a different promise -
            // they are what every connect signs in with - and a reconnect still uses them.
            _rememberedLogin = null;

            if (State == ConnectionState.Authenticated)
            {
                SetConnectionState(ConnectionState.Connected);
            }
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
    public async Task<AuthResponse?> LoginWithPersonalAccessTokenAsync(string token, CancellationToken ct = default)
    {
        var identity = await LoginRegisterAsync(CommandCodes.LOGIN_REGISTER_WITH_PAT_CODE,
            LoginRegister.SerializeWithPersonalAccessToken(token), ct);
        _rememberedLogin = new AutoLoginSettings { Enabled = true, PersonalAccessToken = token };

        return identity;
    }

    /// <summary>
    ///     Connects, optionally without the configured auto login. A caller that authenticates itself right
    ///     after the connect passes <c>false</c>, so the connect does not spend a round trip on credentials the
    ///     caller is about to replace.
    /// </summary>
    private async Task ConnectAsync(bool autoLogin, CancellationToken token)
    {
        if (State is ConnectionState.Connected
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
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (State is ConnectionState.Connected
                or ConnectionState.Authenticating
                or ConnectionState.Authenticated)
            {
                return;
            }

            // The initial delay paces reconnects to the one endpoint a single-address client has. With other
            // endpoints known there is somewhere else to go, and pausing first only pushes the failover past the
            // window the caller is willing to wait; the dial loop's own delay still paces the retries.
            if (_lastConnectionTime != DateTimeOffset.MinValue && DialCandidates().Length == 1)
            {
                await Task.Delay(_configuration.ReconnectionSettings.InitialDelay, token);
            }

            SetConnectionState(ConnectionState.Connecting);
            await TryEstablishConnectionAsync(autoLogin, token);

            // Dispose is synchronous and cannot take the sending semaphore, so a Dispose that ran while this
            // connect was dialing already read a connection that did not exist yet. Reading the flag after the
            // connection is installed leaves no window: either Dispose sees the new connection and closes it, or
            // it is seen here and the connection is dropped.
            if (_disposed)
            {
                await DropConnectionAsync();
                throw new ObjectDisposedException(nameof(TcpMessageStream));
            }

            StartHeartbeat();
        }
        finally
        {
            Interlocked.Exchange(ref _isConnecting, 0);
            _connectGate.Release();
        }
    }

    /// <summary>
    ///     Starts the idle ping loop. Called under <see cref="_connectGate" />, so a reconnect finds the loop
    ///     already running and does not start a second one. A loop that ended anyway - an interval the caller
    ///     mutated out of range faults <see cref="PeriodicTimer" />'s constructor - is started again rather than
    ///     leaving the client without a heartbeat for the rest of its life.
    /// </summary>
    private void StartHeartbeat()
    {
        if (_disposed)
        {
            return;
        }

        if (_heartbeatTask is null or { IsCompleted: true })
        {
            _heartbeatTask = RunHeartbeatAsync(_configuration.HeartbeatInterval, _heartbeatCancellation.Token);
        }
    }

    /// <summary>
    ///     Pings on a timer so the server keeps hearing from an otherwise idle connection. The ping goes through
    ///     the regular request path, so a lost session is repaired by the same reconnect and auto login every
    ///     other request would trigger; the request deadline bounds a stalled server. Failures are logged and
    ///     the loop keeps going until the client is disposed.
    /// </summary>
    private async Task RunHeartbeatAsync(TimeSpan interval, CancellationToken token)
    {
        try
        {
            using var timer = new PeriodicTimer(interval);
            while (await timer.WaitForNextTickAsync(token))
            {
                // Without reconnection and auto login a ping on a dead connection can only fail; with them,
                // the ping is what brings an idle client back.
                var unrecoverable = State is ConnectionState.Disconnected or ConnectionState.Connecting
                                    && !(_configuration.ReconnectionSettings.Enabled
                                         && SignInSettings() != null);
                if (IsConnecting || unrecoverable)
                {
                    continue;
                }

                try
                {
                    await PingAsync(token);
                }
                catch (Exception e) when (!token.IsCancellationRequested && !_disposed)
                {
                    _logger.LogWarning(e, "Heartbeat failed");
                }
            }
        }
        catch (Exception) when (token.IsCancellationRequested || _disposed)
        {
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Heartbeat stopped");
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
                payload.AsMemory(0, messageBufferSize), token: token);
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

        return SendConfirmedAndDisposeAsync(payloadBuffer, bodySize, token);
    }

    private async Task<SendMessagesResponse> SendConfirmedAndDisposeAsync(IMemoryOwner<byte> payloadBuffer,
        int bodySize, CancellationToken token)
    {
        try
        {
            using IMemoryOwner<byte> responseBuffer = await SendWithResponseAsync(CommandCodes.SEND_MESSAGES_CODE,
                payloadBuffer.Memory[..bodySize], token: token);
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

    private async Task TryEstablishConnectionAsync(bool autoLogin, CancellationToken token)
    {
        var retryCount = 0;
        var redirects = 0;
        var delay = _configuration.ReconnectionSettings.InitialDelay;
        Exception? configurationFault = null;

        if (string.IsNullOrEmpty(_currentAddress))
        {
            _currentAddress = _configuration.BaseAddress;
        }

        var candidates = DialCandidates();
        var candidate = 0;
        do
        {
            await DropConnectionAsync();

            if (!ServerAddress.TryParse(_currentAddress, out var host, out var port))
            {
                throw new InvalidBaseAddressException();
            }

            Socket? socket = null;
            var established = false;
            try
            {
                socket = new Socket(ServerAddress.AddressFamilyOf(host), SocketType.Stream, ProtocolType.Tcp);
                socket.SendBufferSize = _configuration.SendBufferSize;
                socket.ReceiveBufferSize = _configuration.ReceiveBufferSize;

                // The protocol is request/reply, so a write is always the last one before
                // the client blocks on the answer and Nagle has nothing to coalesce it with - it only delays the
                // trailing segment of a large request until the previous one is acked.
                socket.NoDelay = true;

                // Neither ConnectAsync nor the TLS handshake has a deadline of its own, and nothing up the
                // stack adds one: a node whose syns are dropped - or one that accepts TCP and then never
                // answers the ClientHello - would hold the sweep, and the connection semaphore with it, for
                // the whole kernel connect timeout while a survivor goes untried.
                using var dialCancellation = candidates.Length > 1
                    ? CancellationTokenSource.CreateLinkedTokenSource(token)
                    : null;
                dialCancellation?.CancelAfter(FailoverDialTimeout);
                var dialToken = dialCancellation?.Token ?? token;

                await socket.ConnectAsync(host, port, dialToken);

                _currentRemoteAddress = socket.RemoteEndPoint is IPEndPoint remote
                    ? ServerAddress.HostPort(remote.Address.ToString(), (ushort)remote.Port)
                    : string.Empty;
                _connectedAddress = _currentAddress;

                socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
                socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveTime, 5);

                var connectionStream = _configuration.TlsSettings.Enabled
                    ? await CreateSslStreamAndAuthenticate(socket, _configuration.TlsSettings, dialToken)
                    : new NetworkStream(socket, true);

                // Established, not merely dialed: everything up to here belongs to this endpoint and the
                // sweep may try the next one, while everything past it - auto login, a redirect, the leader
                // lookup - fails the same way wherever the client lands.
                established = true;

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

                SetConnectionState(ConnectionState.Connected);
                _lastConnectionTime = DateTimeOffset.UtcNow;

                socket = null;

                // No pre-login roster read: the server auth-gates cluster metadata, so leadership settles after
                // a sign-in binds a session. A login dialed at a backup still succeeds because the server
                // forwards the register to the primary.
                if (autoLogin && SignInSettings() is { } signInSettings)
                {
                    await AutoLoginAsync(signInSettings, token);

                    if (await RedirectAsync(token))
                    {
                        await BackoffOrThrowAsync();
                        continue;
                    }
                }

                break;
            }
            // Only bringing an endpoint up is worth trying elsewhere. Everything past it - bad credentials, a
            // leader that cannot be found - fails the same way wherever the client lands, and with unlimited
            // retries a caller would otherwise never get the error back.
            //
            // A handshake the dial bound cut short is a failed attempt on this endpoint like any other, so it
            // must not land here: only a cancellation the caller actually asked for is fatal.
            catch (Exception e) when (established
                                      || (e is OperationCanceledException && token.IsCancellationRequested)
                                      || _disposed)
            {
                socket?.Dispose();

                // The connection is already installed by the time auto login, a redirect or the leader lookup
                // can fail, and this path leaves the loop for good, so nothing else would ever close it.
                await DropConnectionAsync();

                _logger.LogError(e, "Failed to establish connection");
                SetConnectionState(ConnectionState.Disconnected);
                throw;
            }
            catch (Exception e)
            {
                socket?.Dispose();

                _logger.LogError(e, "Failed to connect");

                if (IsTlsConfigurationFault(e))
                {
                    // A fault no retry can fix, kept aside rather than thrown at once: it belongs to the
                    // endpoint that raised it - a CA file that cannot be read - and the endpoints behind that
                    // one may be perfectly usable.
                    configurationFault = e;
                }

                // Every other endpoint gets its turn before the retry delay: the node just lost may be gone for
                // good, and pausing on it helps nothing.
                if (++candidate < candidates.Length)
                {
                    _currentAddress = candidates[candidate];
                    continue;
                }

                candidate = 0;
                _currentAddress = candidates[0];

                // No endpoint answered and at least one said why in a way no retry changes: an unreadable CA
                // file. The caller gets that reason instead of a retry loop that buries it - unlimited retries
                // would otherwise redial it forever.
                if (configurationFault is not null)
                {
                    SetConnectionState(ConnectionState.Disconnected);
                    throw configurationFault;
                }

                // The sweep is what the reconnection budget applies to, not a single dial: checked per dial,
                // the last round would try only the endpoint the client started on, and a client with
                // reconnection turned off would never reach its other endpoints at all.
                if (!_configuration.ReconnectionSettings.Enabled ||
                    (_configuration.ReconnectionSettings.MaxRetries > 0 &&
                     retryCount >= _configuration.ReconnectionSettings.MaxRetries))
                {
                    SetConnectionState(ConnectionState.Disconnected);
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
                SetConnectionState(ConnectionState.Disconnected);
                throw new MissingLeaderException();
            }

            _logger.LogInformation("Following leader redirect {Redirect} to {Address}", redirects, _currentAddress);

            // The redirect moved the client, so the endpoint it moved to leads the next dial.
            candidates = DialCandidates();
            candidate = 0;

            await Task.Delay(delay, token);
        }
    }

    /// <summary>
    ///     Closes the current connection and forgets the consensus session bound to it. Takes the sending
    ///     semaphore, which owns every write to <see cref="_connection" />, so an in-flight request never
    ///     observes the field changing between its write and its reply. Never cancellable: a caller giving up is
    ///     exactly when the connection has to be released.
    /// </summary>
    private async Task DropConnectionAsync()
    {
        await _sendingSemaphore.WaitAsync(CancellationToken.None);
        try
        {
            _connection?.Dispose();
            _connection = null;

            ResetConsensusSession();
        }
        finally
        {
            _sendingSemaphore.Release();
        }
    }

    private string[] DialCandidates()
    {
        return DialCandidates(_currentAddress, _configuration.BaseAddress, _rosterAddresses);
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

    private async Task AutoLoginAsync(AutoLoginSettings settings, CancellationToken token)
    {
        if (!string.IsNullOrEmpty(settings.PersonalAccessToken))
        {
            _logger.LogInformation("Signing in with a personal access token");
            await LoginWithPersonalAccessTokenAsync(settings.PersonalAccessToken, token);
            return;
        }

        _logger.LogInformation("Signing in with credentials: {Username}", settings.Username);
        await LoginUserAsync(settings.Username, settings.Password, token);
    }

    /// <summary>
    ///     The credentials a connect signs in with: the configured ones, or else the ones a sign-in on this client
    ///     succeeded with. Null when nothing has ever signed in, which is when a reconnect cannot restore a
    ///     session at all.
    /// </summary>
    private AutoLoginSettings? SignInSettings()
    {
        if (_configuration.AutoLoginSettings.Enabled)
        {
            return _configuration.AutoLoginSettings;
        }

        return _rememberedLogin;
    }

    private async Task<Stream> CreateSslStreamAndAuthenticate(Socket socket, TlsSettings tlsSettings,
        CancellationToken token)
    {
        ValidateCertificatePath(tlsSettings.CertificatePath);

        _customCaStore = new X509Certificate2Collection();
        _customCaStore.ImportFromPemFile(tlsSettings.CertificatePath);
        var stream = new NetworkStream(socket, true);
        var sslStream = new SslStream(stream, false, RemoteCertificateValidationCallback);
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

    /// <summary>
    ///     Whether bringing an endpoint up failed for a reason that says this client's own TLS configuration is
    ///     wrong: a CA file that cannot be read. It does not change on a retry, so the sweep reports it instead
    ///     of redialing forever.
    /// </summary>
    private static bool IsTlsConfigurationFault(Exception e)
    {
        // A handshake verdict describes the peer, not this client: a certificate that names another host, a
        // peer that answers a ClientHello with something else. The endpoints behind it may be fine, and with
        // one roster entry per node the target host is a bare address no certificate has to cover, so a failed
        // handshake ends the dial rather than the connect.
        return e is InvalidCertificatePathException;
    }

    private async Task SendAckAsync(int code, ReadOnlyMemory<byte> body, CancellationToken token)
    {
        using IMemoryOwner<byte> _ = await SendWithResponseAsync(code, body, token: token);
    }

    private async Task<IMemoryOwner<byte>> SendWithResponseAsync(int code, ReadOnlyMemory<byte> body,
        bool autoLoginOnReconnect = true, CancellationToken token = default)
    {
        try
        {
            return await SendRawAsync(code, body, token);
        }
        catch (Exception e) when (IsLostConnection(e) && !IsConnecting && !_disposed)
        {
            _logger.LogWarning("Connection lost");

            // A stale-client eviction is not caller intent: the server's heartbeat verifier sends it after a
            // gc pause or a laptop sleep, so the remembered sign-in survives it and the reconnect below
            // re-establishes the session. Only an explicit sign-out or Dispose ends it. Same rule in every SDK.
            if (!_configuration.ReconnectionSettings.Enabled)
            {
                _logger.LogWarning("Reconnection is disabled");
                SetConnectionState(ConnectionState.Disconnected);
                throw;
            }

            // With no credentials - neither configured nor remembered from a sign-in - a reconnect cannot
            // re-establish the session, so the request would only come back unauthenticated. Login and register
            // are the exception: they re-authenticate themselves.
            if (SignInSettings() == null && autoLoginOnReconnect)
            {
                _logger.LogWarning("No credentials to sign in with, the session cannot be re-established");
                SetConnectionState(ConnectionState.Disconnected);
                throw;
            }

            return await HandleReconnectionAsync(code, body, autoLoginOnReconnect, token);
        }
    }

    // A stale-client eviction is the server telling this connection its session is gone: the transport already
    // dropped the connection, so the request is replayed over a fresh one like any other lost request.
    private static bool IsLostConnection(Exception e)
    {
        return VsrConnection.IsConnectionException(e)
               || e is IggyInvalidStatusCodeException { StatusCode: VsrError.STALE_CLIENT, FromServer: true };
    }

    private async Task<IMemoryOwner<byte>> HandleReconnectionAsync(int code, ReadOnlyMemory<byte> body,
        bool autoLogin, CancellationToken token)
    {
        var currentTime = DateTimeOffset.UtcNow;
        await _connectionSemaphore.WaitAsync(token);

        try
        {
            if (State is ConnectionState.Connected or ConnectionState.Authenticated
                && _lastConnectionTime > currentTime)
            {
                _logger.LogInformation("Connection already established, sending payload");
                return await SendRawAsync(code, body, token);
            }

            SetConnectionState(ConnectionState.Disconnected);
            _logger.LogInformation("Reconnecting to the server");
            await ConnectAsync(autoLogin, token);

            _logger.LogInformation("Reconnected to the server");

            await Task.Delay(_configuration.ReconnectionSettings.WaitAfterReconnect, token);

            return await SendRawAsync(code, body, token);
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
    ///     aggregator. Callers reach this holding different locks - the connect loop the connection semaphore, a drop the
    ///     sending one, the login path neither - so the swap has to be atomic: a read-modify-write would let a concurrent
    ///     transition publish a previous state that never preceded the current one. Only the thread that changed the value
    ///     publishes, so a state is never announced twice.
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
