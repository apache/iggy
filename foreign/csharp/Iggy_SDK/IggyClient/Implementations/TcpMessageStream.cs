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
using System.Threading.Channels;
using Apache.Iggy.Configuration;
using Apache.Iggy.ConnectionStream;
using Apache.Iggy.Contracts;
using Apache.Iggy.Contracts.Auth;
using Apache.Iggy.Contracts.Tcp;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Headers;
using Apache.Iggy.Kinds;
using Apache.Iggy.Mappers;
using Apache.Iggy.Messages;
using Apache.Iggy.MessagesDispatcher;
using Apache.Iggy.Utils;
using Microsoft.Extensions.Logging;

namespace Apache.Iggy.IggyClient.Implementations;

public sealed class TcpMessageStream : IIggyClient, IDisposable
{
    private readonly Channel<MessageSendRequest>? _channel;
    private readonly ILogger<TcpMessageStream> _logger;
    private readonly IMessageInvoker? _messageInvoker;
    private readonly MessagePollingSettings _messagePollingSettings;
    private readonly IConnectionStream _stream;

    internal TcpMessageStream(IConnectionStream stream, Channel<MessageSendRequest>? channel,
        MessagePollingSettings messagePollingSettings, ILoggerFactory loggerFactory,
        IMessageInvoker? messageInvoker = null)
    {
        _stream = stream;
        _channel = channel;
        _messagePollingSettings = messagePollingSettings;
        _messageInvoker = messageInvoker;
        _logger = loggerFactory.CreateLogger<TcpMessageStream>();
    }

    public void Dispose()
    {
        _stream.Close();
        _stream.Dispose();
    }

    public async Task<StreamResponse?> CreateStreamAsync(string name, uint? streamId, CancellationToken token = default)
    {
        var message = TcpContracts.CreateStream(name, streamId);
        var payload = new byte[4 + BufferSizes.InitialBytesLength + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.CREATE_STREAM_CODE);

        await _stream.SendAsync(payload, token);
        await _stream.FlushAsync(token);

        var responseBuffer = await GetMessageAsync(token);

        if (responseBuffer.Length == 0)
        {
            throw new InvalidResponseException("Received empty response while trying to create stream.");
        }

        return BinaryMapper.MapStream(responseBuffer);
    }

    public async Task<StreamResponse?> GetStreamByIdAsync(Identifier streamId, CancellationToken token = default)
    {
        var message = TcpMessageStreamHelpers.GetBytesFromIdentifier(streamId);
        var payload = new byte[4 + BufferSizes.InitialBytesLength + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.GET_STREAM_CODE);

        await _stream.SendAsync(payload, token);
        await _stream.FlushAsync(token);

        var responseBuffer = await GetMessageAsync(token);

        if (responseBuffer.Length == 0)
        {
            throw new InvalidResponseException("Received empty response while trying to get stream by ID.");
        }

        return BinaryMapper.MapStream(responseBuffer);
    }

    public async Task<IReadOnlyList<StreamResponse>> GetStreamsAsync(CancellationToken token = default)
    {
        var message = Array.Empty<byte>();
        var payload = new byte[4 + BufferSizes.InitialBytesLength + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.GET_STREAMS_CODE);

        await _stream.SendAsync(payload, token);
        await _stream.FlushAsync(token);

        var responseBuffer = await GetMessageAsync(token);

        if (responseBuffer.Length == 0)
        {
            return [];
        }

        return BinaryMapper.MapStreams(responseBuffer);
    }

    public async Task UpdateStreamAsync(Identifier streamId, string name, CancellationToken token = default)
    {
        var message = TcpContracts.UpdateStream(streamId, name);
        var payload = new byte[4 + BufferSizes.InitialBytesLength + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.UPDATE_STREAM_CODE);

        await _stream.SendAsync(payload, token);
        await _stream.FlushAsync(token);

        await CheckResponseAsync(token);
    }

    public async Task PurgeStreamAsync(Identifier streamId, CancellationToken token = default)
    {
        var message = TcpMessageStreamHelpers.GetBytesFromIdentifier(streamId);
        var payload = new byte[4 + BufferSizes.InitialBytesLength + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.PURGE_STREAM_CODE);

        await _stream.SendAsync(payload, token);
        await _stream.FlushAsync(token);

        await CheckResponseAsync(token);
    }

    public async Task DeleteStreamAsync(Identifier streamId, CancellationToken token = default)
    {
        var message = TcpMessageStreamHelpers.GetBytesFromIdentifier(streamId);
        var payload = new byte[4 + BufferSizes.InitialBytesLength + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.DELETE_STREAM_CODE);

        await _stream.SendAsync(payload, token);
        await _stream.FlushAsync(token);

        await CheckResponseAsync(token);
    }

    public async Task<IReadOnlyList<TopicResponse>> GetTopicsAsync(Identifier streamId,
        CancellationToken token = default)
    {
        var message = TcpMessageStreamHelpers.GetBytesFromIdentifier(streamId);
        var payload = new byte[4 + BufferSizes.InitialBytesLength + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.GET_TOPICS_CODE);

        await _stream.SendAsync(payload, token);
        await _stream.FlushAsync(token);

        var responseBuffer = await GetMessageAsync(token);

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
        var payload = new byte[4 + BufferSizes.InitialBytesLength + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.GET_TOPIC_CODE);

        await _stream.SendAsync(payload, token);
        await _stream.FlushAsync(token);

        var responseBuffer = await GetMessageAsync(token);

        if (responseBuffer.Length == 0)
        {
            throw new InvalidResponseException("Received empty response while trying to get topic by ID.");
        }

        return BinaryMapper.MapTopic(responseBuffer);
    }


    public async Task<TopicResponse?> CreateTopicAsync(Identifier streamId, string name, uint partitionsCount,
        CompressionAlgorithm compressionAlgorithm = CompressionAlgorithm.None,
        uint? topicId = null, byte? replicationFactor = null, ulong messageExpiry = 0, ulong maxTopicSize = 0,
        CancellationToken token = default)
    {
        var message = TcpContracts.CreateTopic(streamId, name, partitionsCount, compressionAlgorithm, topicId,
            replicationFactor, messageExpiry, maxTopicSize);
        var payload = new byte[4 + BufferSizes.InitialBytesLength + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.CREATE_TOPIC_CODE);

        await _stream.SendAsync(payload, token);
        await _stream.FlushAsync(token);

        var responseBuffer = await GetMessageAsync(token);

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
        var payload = new byte[4 + BufferSizes.InitialBytesLength + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.UPDATE_TOPIC_CODE);

        await _stream.SendAsync(payload, token);
        await _stream.FlushAsync(token);

        await CheckResponseAsync(token);
    }

    public async Task DeleteTopicAsync(Identifier streamId, Identifier topicId, CancellationToken token = default)
    {
        var message = TcpContracts.DeleteTopic(streamId, topicId);
        var payload = new byte[4 + BufferSizes.InitialBytesLength + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.DELETE_TOPIC_CODE);

        await _stream.SendAsync(payload, token);
        await _stream.FlushAsync(token);

        await CheckResponseAsync(token);
    }

    public async Task PurgeTopicAsync(Identifier streamId, Identifier topicId, CancellationToken token = default)
    {
        var message = TcpContracts.PurgeTopic(streamId, topicId);
        var payload = new byte[4 + BufferSizes.InitialBytesLength + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.PURGE_TOPIC_CODE);

        await _stream.SendAsync(payload, token);
        await _stream.FlushAsync(token);

        await CheckResponseAsync(token);
    }

    public async Task SendMessagesAsync(MessageSendRequest request,
        Func<byte[], byte[]>? encryptor = null,
        CancellationToken token = default)
    {
        if (request.Messages.Count == 0)
        {
            return;
        }

        //TODO - explore making fields of Message class mutable, so there is no need to create em from scratch
        if (encryptor is not null)
        {
            for (var i = 0; i < request.Messages.Count || token.IsCancellationRequested; i++)
            {
                request.Messages[i] = request.Messages[i] with { Payload = encryptor(request.Messages[i].Payload) };
            }
        }

        if (_messageInvoker is not null)
        {
            await _messageInvoker.SendMessagesAsync(request, token);
            return;
        }

        await _channel!.Writer.WriteAsync(request, token);
    }

    // TODO: Change TMessage implementation
    public async Task SendMessagesAsync<TMessage>(MessageSendRequest<TMessage> request,
        Func<TMessage, byte[]> serializer,
        Func<byte[], byte[]>? encryptor = null,
        Dictionary<HeaderKey, HeaderValue>? headers = null,
        CancellationToken token = default)
    {
        IList<TMessage> messages = request.Messages;
        if (messages.Count == 0)
        {
            return;
        }

        //TODO - explore making fields of Message class mutable, so there is no need to create em from scratch
        var messagesBuffer = new Message[messages.Count];
        for (var i = 0; i < messages.Count || token.IsCancellationRequested; i++)
        {
            var payload = encryptor is not null ? encryptor(serializer(messages[i])) : serializer(messages[i]);
            var checksum = BitConverter.ToUInt64(Crc64.Hash(payload));

            messagesBuffer[i] = new Message
            {
                Payload = payload,
                Header = new MessageHeader
                {
                    Id = 0,
                    Checksum = checksum,
                    Offset = 0,
                    OriginTimestamp = 0,
                    Timestamp = DateTimeOffset.UtcNow,
                    PayloadLength = payload.Length,
                    UserHeadersLength = 0
                },
                UserHeaders = headers
            };
        }

        var sendRequest = new MessageSendRequest
        {
            StreamId = request.StreamId,
            TopicId = request.TopicId,
            Partitioning = request.Partitioning,
            Messages = messagesBuffer
        };

        if (_messageInvoker is not null)
        {
            await _messageInvoker.SendMessagesAsync(sendRequest, token);
            return;
        }

        await _channel!.Writer.WriteAsync(sendRequest, token);
    }

    public async Task FlushUnsavedBufferAsync(Identifier streamId, Identifier topicId, uint partitionId, bool fsync,
        CancellationToken token = default)
    {
        var message = TcpContracts.FlushUnsavedBuffer(streamId, topicId, partitionId, fsync);
        ;
        var payload = new byte[4 + BufferSizes.InitialBytesLength + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.FLUSH_UNSAVED_BUFFER_CODE);

        await _stream.SendAsync(payload, token);
        await _stream.FlushAsync(token);

        await CheckResponseAsync(token);
    }

    public async Task<PolledMessages<TMessage>> PollMessagesAsync<TMessage>(MessageFetchRequest request,
        Func<byte[], TMessage> serializer, Func<byte[], byte[]>? decryptor = null, CancellationToken token = default)
    {
        await SendFetchMessagesRequestPayload(request.Consumer, request.StreamId, request.TopicId,
            request.PollingStrategy,
            request.Count, request.AutoCommit, request.PartitionId, token);
        IMemoryOwner<byte> buffer = MemoryPool<byte>.Shared.Rent(BufferSizes.ExpectedResponseSize);
        try
        {
            await _stream.ReadAsync(buffer.Memory[..BufferSizes.ExpectedResponseSize], token);
            var response = TcpMessageStreamHelpers.GetResponseLengthAndStatus(buffer.Memory.Span);
            if (response.Status != 0)
            {
                if (response.Length == 0)
                {
                    throw new InvalidResponseException($"Invalid response status code: {response.Status}");
                }

                var errorBuffer = new byte[response.Length];
                await _stream.ReadAsync(errorBuffer, token);
                throw new InvalidResponseException(Encoding.UTF8.GetString(errorBuffer));
            }

            if (response.Length <= 1)
            {
                return PolledMessages<TMessage>.Empty;
            }

            IMemoryOwner<byte> responseBuffer = MemoryPool<byte>.Shared.Rent(response.Length);

            try
            {
                await _stream.ReadAsync(responseBuffer.Memory[..response.Length], token);
                PolledMessages<TMessage> result = BinaryMapper.MapMessages(
                    responseBuffer.Memory.Span[..response.Length], serializer, decryptor);
                return result;
            }
            finally
            {
                responseBuffer.Dispose();
            }
        }
        finally
        {
            buffer.Dispose();
        }
    }

    public async IAsyncEnumerable<MessageResponse<TMessage>> PollMessagesAsync<TMessage>(PollMessagesRequest request,
        Func<byte[], TMessage> deserializer, Func<byte[], byte[]>? decryptor = null,
        [EnumeratorCancellation] CancellationToken token = default)
    {
        var channel = Channel.CreateUnbounded<MessageResponse<TMessage>>();
        var autoCommit = _messagePollingSettings.StoreOffsetStrategy switch
        {
            StoreOffset.Never => false,
            StoreOffset.WhenMessagesAreReceived => true,
            StoreOffset.AfterProcessingEachMessage => false,
            _ => throw new ArgumentOutOfRangeException()
        };
        var fetchRequest = new MessageFetchRequest
        {
            Consumer = request.Consumer,
            StreamId = request.StreamId,
            TopicId = request.TopicId,
            AutoCommit = autoCommit,
            Count = request.Count,
            PartitionId = request.PartitionId,
            PollingStrategy = request.PollingStrategy
        };


        _ = StartPollingMessagesAsync(fetchRequest, deserializer, _messagePollingSettings.Interval, channel.Writer,
            decryptor, token);
        await foreach (MessageResponse<TMessage> messageResponse in channel.Reader.ReadAllAsync(token))
        {
            yield return messageResponse;

            var currentOffset = messageResponse.Header.Offset;
            if (_messagePollingSettings.StoreOffsetStrategy is StoreOffset.AfterProcessingEachMessage)
            {
                try
                {
                    await StoreOffsetAsync(request.Consumer, request.StreamId, request.TopicId, currentOffset,
                        request.PartitionId, token);
                }
                catch
                {
                    _logger.LogError(
                        "Error encountered while saving offset information - Offset: {offset}, Stream ID: {streamId}, Topic ID: {topicId}, Partition ID: {partitionId}",
                        currentOffset, request.StreamId, request.TopicId, request.PartitionId);
                }
            }

            if (request.PollingStrategy.Kind is MessagePolling.Offset)
            {
                //TODO - check with profiler whether this doesn't cause a lot of allocations
                request.PollingStrategy = PollingStrategy.Offset(currentOffset + 1);
            }
        }
    }

    public async Task<PolledMessages> PollMessagesAsync(MessageFetchRequest request,
        Func<byte[], byte[]>? decryptor = null,
        CancellationToken token = default)
    {
        await SendFetchMessagesRequestPayload(request.Consumer, request.StreamId, request.TopicId,
            request.PollingStrategy,
            request.Count, request.AutoCommit, request.PartitionId, token);

        var buffer = ArrayPool<byte>.Shared.Rent(BufferSizes.ExpectedResponseSize);
        try
        {
            await _stream.ReadAsync(buffer.AsMemory()[..BufferSizes.ExpectedResponseSize], token);

            var response = TcpMessageStreamHelpers.GetResponseLengthAndStatus(buffer);
            if (response.Status != 0)
            {
                if (response.Length == 0)
                {
                    throw new InvalidResponseException($"Invalid response status code: {response.Status}");
                }

                var errorBuffer = new byte[response.Length];
                await _stream.ReadAsync(errorBuffer, token);
                throw new InvalidResponseException(Encoding.UTF8.GetString(errorBuffer));
            }

            if (response.Length <= 1)
            {
                return PolledMessages.Empty;
            }

            var responseBuffer = ArrayPool<byte>.Shared.Rent(response.Length);

            try
            {
                await _stream.ReadAsync(responseBuffer.AsMemory()[..response.Length], token);
                var result = BinaryMapper.MapMessages(responseBuffer.AsSpan()[..response.Length], decryptor);
                return result;
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(responseBuffer);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    public async Task StoreOffsetAsync(Consumer consumer, Identifier streamId, Identifier topicId, ulong offset,
        uint? partitionId, CancellationToken token = default)
    {
        var message = TcpContracts.UpdateOffset(streamId, topicId, consumer, offset, partitionId);
        var payload = new byte[4 + BufferSizes.InitialBytesLength + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.STORE_CONSUMER_OFFSET_CODE);

        await _stream.SendAsync(payload, token);
        await _stream.FlushAsync(token);

        await CheckResponseAsync(token);
    }

    public async Task<OffsetResponse?> GetOffsetAsync(Consumer consumer, Identifier streamId, Identifier topicId,
        uint? partitionId, CancellationToken token = default)
    {
        var message = TcpContracts.GetOffset(streamId, topicId, consumer, partitionId);
        var payload = new byte[4 + BufferSizes.InitialBytesLength + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.GET_CONSUMER_OFFSET_CODE);

        await _stream.SendAsync(payload, token);
        await _stream.FlushAsync(token);

        var responseBuffer = await GetMessageAsync(token);

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
        var payload = new byte[4 + BufferSizes.InitialBytesLength + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.DELETE_CONSUMER_OFFSET_CODE);

        await _stream.SendAsync(payload, token);
        await _stream.FlushAsync(token);

        await CheckResponseAsync(token);
    }

    public async Task<IReadOnlyList<ConsumerGroupResponse>> GetConsumerGroupsAsync(Identifier streamId,
        Identifier topicId,
        CancellationToken token = default)
    {
        var message = TcpContracts.GetGroups(streamId, topicId);
        var payload = new byte[4 + BufferSizes.InitialBytesLength + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.GET_CONSUMER_GROUPS_CODE);

        await _stream.SendAsync(payload, token);
        await _stream.FlushAsync(token);

        var responseBuffer = await GetMessageAsync(token);

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
        var payload = new byte[4 + BufferSizes.InitialBytesLength + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.GET_CONSUMER_GROUP_CODE);

        await _stream.SendAsync(payload, token);
        await _stream.FlushAsync(token);

        var responseBuffer = await GetMessageAsync(token);

        if (responseBuffer.Length == 0)
        {
            return null;
        }

        return BinaryMapper.MapConsumerGroup(responseBuffer);
    }

    public async Task<ConsumerGroupResponse?> CreateConsumerGroupAsync(Identifier streamId, Identifier topicId,
        string name, uint? groupId, CancellationToken token = default)
    {
        var message = TcpContracts.CreateGroup(streamId, topicId, name, groupId);
        var payload = new byte[4 + BufferSizes.InitialBytesLength + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.CREATE_CONSUMER_GROUP_CODE);

        await _stream.SendAsync(payload, token);
        await _stream.FlushAsync(token);

        var responseBuffer = await GetMessageAsync(token);

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
        var payload = new byte[4 + BufferSizes.InitialBytesLength + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.DELETE_CONSUMER_GROUP_CODE);

        await _stream.SendAsync(payload, token);
        await _stream.FlushAsync(token);

        await CheckResponseAsync(token);
    }

    public async Task JoinConsumerGroupAsync(Identifier streamId, Identifier topicId, Identifier groupId,
        CancellationToken token = default)
    {
        var message = TcpContracts.JoinGroup(streamId, topicId, groupId);
        var payload = new byte[4 + BufferSizes.InitialBytesLength + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.JOIN_CONSUMER_GROUP_CODE);

        await _stream.SendAsync(payload, token);
        await _stream.FlushAsync(token);

        await CheckResponseAsync(token);
    }

    public async Task LeaveConsumerGroupAsync(Identifier streamId, Identifier topicId, Identifier groupId,
        CancellationToken token = default)
    {
        var message = TcpContracts.LeaveGroup(streamId, topicId, groupId);
        var payload = new byte[4 + BufferSizes.InitialBytesLength + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.LEAVE_CONSUMER_GROUP_CODE);

        await _stream.SendAsync(payload, token);
        await _stream.FlushAsync(token);

        await CheckResponseAsync(token);
    }

    public async Task DeletePartitionsAsync(Identifier streamId, Identifier topicId, uint partitionsCount,
        CancellationToken token = default)
    {
        var message = TcpContracts.DeletePartitions(streamId, topicId, partitionsCount);
        var payload = new byte[4 + BufferSizes.InitialBytesLength + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.DELETE_PARTITIONS_CODE);

        await _stream.SendAsync(payload, token);
        await _stream.FlushAsync(token);

        await CheckResponseAsync(token);
    }

    public async Task CreatePartitionsAsync(Identifier streamId, Identifier topicId, uint partitionsCount,
        CancellationToken token = default)
    {
        var message = TcpContracts.CreatePartitions(streamId, topicId, partitionsCount);
        var payload = new byte[4 + BufferSizes.InitialBytesLength + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.CREATE_PARTITIONS_CODE);

        await _stream.SendAsync(payload, token);
        await _stream.FlushAsync(token);

        await CheckResponseAsync(token);
    }

    public async Task<ClientResponse?> GetMeAsync(CancellationToken token = default)
    {
        var message = Array.Empty<byte>();
        var payload = new byte[4 + BufferSizes.InitialBytesLength + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.GET_ME_CODE);

        await _stream.SendAsync(payload, token);
        await _stream.FlushAsync(token);

        var responseBuffer = await GetMessageAsync(token);

        if (responseBuffer.Length == 0)
        {
            return null;
        }

        return BinaryMapper.MapClient(responseBuffer);
    }

    public async Task<StatsResponse?> GetStatsAsync(CancellationToken token = default)
    {
        var message = Array.Empty<byte>();
        var payload = new byte[4 + BufferSizes.InitialBytesLength + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.GET_STATS_CODE);

        await _stream.SendAsync(payload, token);
        await _stream.FlushAsync(token);

        var responseBuffer = await GetMessageAsync(token);

        if (responseBuffer.Length == 0)
        {
            return null;
        }

        return BinaryMapper.MapStats(responseBuffer);
    }

    public async Task PingAsync(CancellationToken token = default)
    {
        var message = Array.Empty<byte>();
        var payload = new byte[4 + BufferSizes.InitialBytesLength + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.PING_CODE);

        await _stream.SendAsync(payload, token);
        await _stream.FlushAsync(token);

        await CheckResponseAsync(token);
    }

    public async Task<IReadOnlyList<ClientResponse>> GetClientsAsync(CancellationToken token = default)
    {
        var message = Array.Empty<byte>();
        var payload = new byte[4 + BufferSizes.InitialBytesLength + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.GET_CLIENTS_CODE);

        await _stream.SendAsync(payload, token);
        await _stream.FlushAsync(token);

        var responseBuffer = await GetMessageAsync(token);

        if (responseBuffer.Length == 0)
        {
            return [];
        }

        return BinaryMapper.MapClients(responseBuffer);
    }

    public async Task<ClientResponse?> GetClientByIdAsync(uint clientId, CancellationToken token = default)
    {
        var message = TcpContracts.GetClient(clientId);
        var payload = new byte[4 + BufferSizes.InitialBytesLength + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.GET_CLIENT_CODE);

        await _stream.SendAsync(payload, token);
        await _stream.FlushAsync(token);

        var responseBuffer = await GetMessageAsync(token);

        if (responseBuffer.Length == 0)
        {
            return null;
        }

        return BinaryMapper.MapClient(responseBuffer);
    }

    public async Task<UserResponse?> GetUser(Identifier userId, CancellationToken token = default)
    {
        var message = TcpContracts.GetUser(userId);
        var payload = new byte[4 + BufferSizes.InitialBytesLength + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.GET_USER_CODE);

        await _stream.SendAsync(payload, token);
        await _stream.FlushAsync(token);

        var responseBuffer = await GetMessageAsync(token);

        if (responseBuffer.Length == 0)
        {
            return null;
        }

        return BinaryMapper.MapUser(responseBuffer);
    }

    public async Task<IReadOnlyList<UserResponse>> GetUsers(CancellationToken token = default)
    {
        var message = Array.Empty<byte>();
        var payload = new byte[4 + BufferSizes.InitialBytesLength + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.GET_USERS_CODE);

        await _stream.SendAsync(payload, token);
        await _stream.FlushAsync(token);

        var responseBuffer = await GetMessageAsync(token);

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
        var payload = new byte[4 + BufferSizes.InitialBytesLength + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.CREATE_USER_CODE);

        await _stream.SendAsync(payload, token);
        await _stream.FlushAsync(token);

        var responseBuffer = await GetMessageAsync(token);

        if (responseBuffer.Length == 0)
        {
            return null;
        }

        return BinaryMapper.MapUser(responseBuffer);
    }

    public async Task DeleteUser(Identifier userId, CancellationToken token = default)
    {
        var message = TcpContracts.DeleteUser(userId);
        var payload = new byte[4 + BufferSizes.InitialBytesLength + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.DELETE_USER_CODE);

        await _stream.SendAsync(payload, token);
        await _stream.FlushAsync(token);

        await CheckResponseAsync(token);
    }

    public async Task UpdateUser(Identifier userId, string? userName = null, UserStatus? status = null,
        CancellationToken token = default)
    {
        var message = TcpContracts.UpdateUser(userId, userName, status);
        var payload = new byte[4 + BufferSizes.InitialBytesLength + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.UPDATE_USER_CODE);

        await _stream.SendAsync(payload, token);
        await _stream.FlushAsync(token);

        await CheckResponseAsync(token);
    }

    public async Task UpdatePermissions(Identifier userId, Permissions? permissions = null,
        CancellationToken token = default)
    {
        var message = TcpContracts.UpdatePermissions(userId, permissions);
        var payload = new byte[4 + BufferSizes.InitialBytesLength + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.UPDATE_PERMISSIONS_CODE);

        await _stream.SendAsync(payload, token);
        await _stream.FlushAsync(token);

        await CheckResponseAsync(token);
    }

    public async Task ChangePassword(Identifier userId, string currentPassword, string newPassword,
        CancellationToken token = default)
    {
        var message = TcpContracts.ChangePassword(userId, currentPassword, newPassword);
        var payload = new byte[4 + BufferSizes.InitialBytesLength + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.CHANGE_PASSWORD_CODE);

        await _stream.SendAsync(payload, token);
        await _stream.FlushAsync(token);

        await CheckResponseAsync(token);
    }

    public async Task<AuthResponse?> LoginUser(string userName, string password, CancellationToken token = default)
    {
        var message = TcpContracts.LoginUser(userName, password, "0.5.0", "csharp-sdk");
        var payload = new byte[4 + BufferSizes.InitialBytesLength + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.LOGIN_USER_CODE);

        await _stream.SendAsync(payload, token);
        await _stream.FlushAsync(token);

        var responseBuffer = await GetMessageAsync(token);

        if (responseBuffer.Length <= 0)
        {
            return null;
        }

        var userId = BinaryPrimitives.ReadInt32LittleEndian(responseBuffer.AsSpan()[..responseBuffer.Length]);

        //TODO: Figure out how to solve this workaround about default of TokenInfo
        return new AuthResponse(userId, null);
    }

    public async Task LogoutUser(CancellationToken token = default)
    {
        var message = Array.Empty<byte>();
        var payload = new byte[4 + BufferSizes.InitialBytesLength + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.LOGOUT_USER_CODE);

        await _stream.SendAsync(payload, token);
        await _stream.FlushAsync(token);

        await CheckResponseAsync(token);
    }

    public async Task<IReadOnlyList<PersonalAccessTokenResponse>> GetPersonalAccessTokensAsync(
        CancellationToken token = default)
    {
        var message = Array.Empty<byte>();
        var payload = new byte[4 + BufferSizes.InitialBytesLength + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.GET_PERSONAL_ACCESS_TOKENS_CODE);

        await _stream.SendAsync(payload, token);
        await _stream.FlushAsync(token);

        var responseBuffer = await GetMessageAsync(token);

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
        var payload = new byte[4 + BufferSizes.InitialBytesLength + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.CREATE_PERSONAL_ACCESS_TOKEN_CODE);

        await _stream.SendAsync(payload, token);
        await _stream.FlushAsync(token);

        var responseBuffer = await GetMessageAsync(token);

        if (responseBuffer.Length == 0)
        {
            return null;
        }

        return BinaryMapper.MapRawPersonalAccessToken(responseBuffer);
    }

    public async Task DeletePersonalAccessTokenAsync(string name, CancellationToken token = default)
    {
        var message = TcpContracts.DeletePersonalRequestToken(name);
        var payload = new byte[4 + BufferSizes.InitialBytesLength + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.DELETE_PERSONAL_ACCESS_TOKEN_CODE);

        await _stream.SendAsync(payload, token);
        await _stream.FlushAsync(token);

        await CheckResponseAsync(token);
    }

    public async Task<AuthResponse?> LoginWithPersonalAccessToken(string token, CancellationToken ct = default)
    {
        var message = TcpContracts.LoginWithPersonalAccessToken(token);
        var payload = new byte[4 + BufferSizes.InitialBytesLength + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE);

        await _stream.SendAsync(payload, ct);
        await _stream.FlushAsync(ct);

        var responseBuffer = await GetMessageAsync(ct);

        if (responseBuffer.Length <= 1)
        {
            return null;
        }

        var userId = BinaryPrimitives.ReadInt32LittleEndian(responseBuffer.AsSpan()[..4]);

        //TODO: Figure out how to solve this workaround about default of TokenInfo
        return new AuthResponse(userId, default);
    }

    //TODO - look into calling the non generic FetchMessagesAsync method in order
    //to make this method re-usable for non generic PollMessages method.
    private async Task StartPollingMessagesAsync<TMessage>(MessageFetchRequest request,
        Func<byte[], TMessage> deserializer, TimeSpan interval, ChannelWriter<MessageResponse<TMessage>> writer,
        Func<byte[], byte[]>? decryptor = null,
        CancellationToken token = default)
    {
        var timer = new PeriodicTimer(interval);
        while (await timer.WaitForNextTickAsync(token) || token.IsCancellationRequested)
        {
            try
            {
                PolledMessages<TMessage> fetchResponse
                    = await PollMessagesAsync(request, deserializer, decryptor, token);
                if (fetchResponse.Messages.Count == 0)
                {
                    continue;
                }

                foreach (MessageResponse<TMessage> messageResponse in fetchResponse.Messages)
                {
                    await writer.WriteAsync(messageResponse, token);
                }
            }
            catch (InvalidResponseException e)
            {
                _logger.LogError(
                    "Error encountered while polling messages - Stream ID: {streamId}, Topic ID: {topicId}, Partition ID: {partitionId}, error message {message}",
                    request.StreamId, request.TopicId, request.PartitionId, e.Message);
            }
        }

        writer.Complete();
    }

    private async Task SendFetchMessagesRequestPayload(Consumer consumer, Identifier streamId, Identifier topicId,
        PollingStrategy pollingStrategy,
        int count, bool autoCommit, uint? partitionId, CancellationToken token)
    {
        var messageBufferSize = CalculateMessageBufferSize(streamId, topicId, consumer);
        var payloadBufferSize = CalculatePayloadBufferSize(messageBufferSize);
        var message = ArrayPool<byte>.Shared.Rent(messageBufferSize);
        var payload = ArrayPool<byte>.Shared.Rent(payloadBufferSize);

        try
        {
            TcpContracts.GetMessages(message.AsSpan()[..messageBufferSize], consumer, streamId, topicId,
                pollingStrategy, count, autoCommit, partitionId);
            TcpMessageStreamHelpers.CreatePayload(payload, message.AsSpan()[..messageBufferSize],
                CommandCodes.POLL_MESSAGES_CODE);

            await _stream.SendAsync(payload.AsMemory()[..payloadBufferSize], token);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(message);
            ArrayPool<byte>.Shared.Return(payload);
        }
    }

    private async Task CheckResponseAsync(CancellationToken token = default)
    {
        var buffer = new byte[BufferSizes.ExpectedResponseSize];
        var readBytes = await _stream.ReadAsync(buffer, token);

        if (readBytes == 0)
        {
            throw new InvalidResponseException("Received empty response from server or connection was closed");
        }

        var response = TcpMessageStreamHelpers.GetResponseLengthAndStatus(buffer);

        if (response.Status != 0)
        {
            if (response.Length == 0)
            {
                throw new InvalidResponseException($"Invalid response status code: {response.Status}");
            }

            var errorBuffer = new byte[response.Length];
            await _stream.ReadAsync(errorBuffer, token);
            throw new InvalidResponseException(Encoding.UTF8.GetString(errorBuffer));
        }

        if (response.Length != 0)
        {
            throw new InvalidResponseException("Expected response length to be 0, but got " + response.Length);
        }
    }

    private async Task<byte[]> GetMessageAsync(CancellationToken token = default)
    {
        var buffer = new byte[BufferSizes.ExpectedResponseSize];
        var readBytes = await _stream.ReadAsync(buffer, token);

        if (readBytes == 0)
        {
            throw new InvalidResponseException("Received empty response from server or connection was closed");
        }

        var response = TcpMessageStreamHelpers.GetResponseLengthAndStatus(buffer);

        if (response.Status != 0)
        {
            if (response.Length == 0)
            {
                throw new InvalidResponseException($"Invalid response status code: {response.Status}");
            }

            var errorBuffer = new byte[response.Length];
            await _stream.ReadAsync(errorBuffer, token);
            throw new InvalidResponseException(Encoding.UTF8.GetString(errorBuffer));
        }

        if (response.Length == 0)
        {
            return [];
        }

        var responseBuffer = new byte[response.Length];
        await _stream.ReadAsync(responseBuffer, token);
        return responseBuffer;
    }

    private static int CalculatePayloadBufferSize(int messageBufferSize)
    {
        return messageBufferSize + 4 + BufferSizes.InitialBytesLength;
    }

    private static int CalculateMessageBufferSize(Identifier streamId, Identifier topicId, Consumer consumer)
    {
        return 14 + 5 + 2 + streamId.Length + 2 + topicId.Length + 2 + consumer.Id.Length;
    }
}