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
using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Channels;
using Apache.Iggy.Configuration;
using Apache.Iggy.Contracts;
using Apache.Iggy.Contracts.Auth;
using Apache.Iggy.Contracts.Http;
using Apache.Iggy.Contracts.Http.Auth;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Headers;
using Apache.Iggy.Kinds;
using Apache.Iggy.Messages;
using Apache.Iggy.MessagesDispatcher;
using Apache.Iggy.StringHandlers;
using Microsoft.Extensions.Logging;

namespace Apache.Iggy.IggyClient.Implementations;

public class HttpMessageStream : IIggyClient
{
    private const string Context = "csharp-sdk";

    private readonly Channel<MessageSendRequest>? _channel;
    private readonly HttpClient _httpClient;

    //TODO - create mechanism for refreshing jwt token
    //TODO - replace the HttpClient with IHttpClientFactory, when implementing support for ASP.NET Core DI
    //TODO - the error handling pattern is pretty ugly, look into moving it into an extension method
    private readonly JsonSerializerOptions _jsonSerializerOptions;
    private readonly ILogger<HttpMessageStream> _logger;
    private readonly IMessageInvoker? _messageInvoker;
    private readonly MessagePollingSettings _messagePollingSettings;

    internal HttpMessageStream(HttpClient httpClient, Channel<MessageSendRequest>? channel,
        MessagePollingSettings messagePollingSettings, ILoggerFactory loggerFactory,
        IMessageInvoker? messageInvoker = null)
    {
        _httpClient = httpClient;
        _channel = channel;
        _messagePollingSettings = messagePollingSettings;
        _messageInvoker = messageInvoker;
        _logger = loggerFactory.CreateLogger<HttpMessageStream>();

        _jsonSerializerOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
            Converters = { new JsonStringEnumConverter(JsonNamingPolicy.SnakeCaseLower) }
        };
    }

    public async Task<StreamResponse?> CreateStreamAsync(string name, uint? streamId = null,
        CancellationToken token = default)
    {
        var json = JsonSerializer.Serialize(new CreateStreamRequest(streamId, name), _jsonSerializerOptions);

        var data = new StringContent(json, Encoding.UTF8, "application/json");

        var response = await _httpClient.PostAsync("/streams", data, token);

        if (response.IsSuccessStatusCode)
        {
            return await response.Content.ReadFromJsonAsync<StreamResponse>(_jsonSerializerOptions, token);
        }

        await HandleResponseAsync(response);

        return null;
    }

    public async Task PurgeStreamAsync(Identifier streamId, CancellationToken token = default)
    {
        var response = await _httpClient.DeleteAsync($"/streams/{streamId}/purge", token);
        if (!response.IsSuccessStatusCode)
        {
            await HandleResponseAsync(response);
        }
    }

    public async Task DeleteStreamAsync(Identifier streamId, CancellationToken token = default)
    {
        var response = await _httpClient.DeleteAsync($"/streams/{streamId}", token);
        if (!response.IsSuccessStatusCode)
        {
            await HandleResponseAsync(response);
        }
    }

    public async Task<StreamResponse?> GetStreamByIdAsync(Identifier streamId, CancellationToken token = default)
    {
        var response = await _httpClient.GetAsync($"/streams/{streamId}", token);

        if (response.IsSuccessStatusCode)
        {
            return await response.Content.ReadFromJsonAsync<StreamResponse>(_jsonSerializerOptions, token);
        }

        await HandleResponseAsync(response);

        return null;
    }

    public async Task UpdateStreamAsync(Identifier streamId, string name, CancellationToken token = default)
    {
        var json = JsonSerializer.Serialize(new UpdateStreamRequest(name), _jsonSerializerOptions);

        var data = new StringContent(json, Encoding.UTF8, "application/json");
        var response = await _httpClient.PutAsync($"/streams/{streamId}", data, token);
        if (!response.IsSuccessStatusCode)
        {
            await HandleResponseAsync(response);
        }
    }

    public async Task<IReadOnlyList<StreamResponse>> GetStreamsAsync(CancellationToken token = default)
    {
        var response = await _httpClient.GetAsync("/streams", token);
        if (response.IsSuccessStatusCode)
        {
            return await response.Content.ReadFromJsonAsync<IReadOnlyList<StreamResponse>>(_jsonSerializerOptions,
                       token)
                   ?? Array.Empty<StreamResponse>();
        }

        await HandleResponseAsync(response);
        return Array.Empty<StreamResponse>();
    }

    public async Task<TopicResponse?> CreateTopicAsync(Identifier streamId, string name, uint partitionsCount,
        CompressionAlgorithm compressionAlgorithm = CompressionAlgorithm.None,
        uint? topicId = null, byte? replicationFactor = null, ulong messageExpiry = 0, ulong maxTopicSize = 0,
        CancellationToken token = default)
    {
        var json = JsonSerializer.Serialize(new CreateTopicRequest
        {
            Name = name,
            CompressionAlgorithm = compressionAlgorithm,
            MaxTopicSize = maxTopicSize,
            MessageExpiry = messageExpiry,
            PartitionsCount = partitionsCount,
            ReplicationFactor = replicationFactor,
            TopicId = topicId
        }, _jsonSerializerOptions);
        var data = new StringContent(json, Encoding.UTF8, "application/json");

        var response = await _httpClient.PostAsync($"/streams/{streamId}/topics", data, token);

        if (response.IsSuccessStatusCode)
        {
            return await response.Content.ReadFromJsonAsync<TopicResponse>(_jsonSerializerOptions, token);
        }

        await HandleResponseAsync(response);

        return null;
    }

    public async Task UpdateTopicAsync(Identifier streamId, Identifier topicId, string name,
        CompressionAlgorithm compressionAlgorithm = CompressionAlgorithm.None,
        ulong maxTopicSize = 0, ulong messageExpiry = 0, byte? replicationFactor = null,
        CancellationToken token = default)
    {
        var json = JsonSerializer.Serialize(
            new UpdateTopicRequest(name, compressionAlgorithm, maxTopicSize, messageExpiry, replicationFactor),
            _jsonSerializerOptions);
        var data = new StringContent(json, Encoding.UTF8, "application/json");
        var response = await _httpClient.PutAsync($"/streams/{streamId}/topics/{topicId}", data, token);
        if (!response.IsSuccessStatusCode)
        {
            await HandleResponseAsync(response);
        }
    }

    public async Task DeleteTopicAsync(Identifier streamId, Identifier topicId, CancellationToken token = default)
    {
        var response = await _httpClient.DeleteAsync($"/streams/{streamId}/topics/{topicId}", token);
        if (!response.IsSuccessStatusCode)
        {
            await HandleResponseAsync(response);
        }
    }

    public Task PurgeTopicAsync(Identifier streamId, Identifier topicId, CancellationToken token = default)
    {
        return _httpClient.DeleteAsync($"/streams/{streamId}/topics/{topicId}/purge", token)
            .ContinueWith(async response =>
            {
                if (!response.Result.IsSuccessStatusCode)
                {
                    await HandleResponseAsync(response.Result);
                }
            }, token);
    }

    public async Task<IReadOnlyList<TopicResponse>> GetTopicsAsync(Identifier streamId,
        CancellationToken token = default)
    {
        var response = await _httpClient.GetAsync($"/streams/{streamId}/topics", token);
        if (response.IsSuccessStatusCode)
        {
            return await response.Content.ReadFromJsonAsync<IReadOnlyList<TopicResponse>>(_jsonSerializerOptions, token)
                   ?? Array.Empty<TopicResponse>();
        }

        await HandleResponseAsync(response);
        return Array.Empty<TopicResponse>();
    }

    public async Task<TopicResponse?> GetTopicByIdAsync(Identifier streamId, Identifier topicId,
        CancellationToken token = default)
    {
        var response = await _httpClient.GetAsync($"/streams/{streamId}/topics/{topicId}", token);

        if (response.IsSuccessStatusCode)
        {
            return await response.Content.ReadFromJsonAsync<TopicResponse>(_jsonSerializerOptions, token);
        }

        await HandleResponseAsync(response);

        return null;
    }

    public async Task SendMessagesAsync(MessageSendRequest request,
        Func<byte[], byte[]>? encryptor = null,
        CancellationToken token = default)
    {
        if (encryptor is not null)
        {
            for (var i = 0; i < request.Messages.Count; i++)
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

    public async Task SendMessagesAsync<TMessage>(MessageSendRequest<TMessage> request,
        Func<TMessage, byte[]> serializer,
        Func<byte[], byte[]>? encryptor = null, Dictionary<HeaderKey, HeaderValue>? headers = null,
        CancellationToken token = default)
    {
        IList<TMessage> messages = request.Messages;
        //TODO - maybe get rid of this closure ?
        var sendRequest = new MessageSendRequest
        {
            StreamId = request.StreamId,
            TopicId = request.TopicId,
            Partitioning = request.Partitioning,
            Messages = messages.Select(message =>
            {
                return new Message
                {
                    // TODO: message id
                    Header = new MessageHeader { Id = 0 },
                    UserHeaders = headers,
                    Payload = encryptor is not null ? encryptor(serializer(message)) : serializer(message)
                };
            }).ToArray()
        };

        if (_messageInvoker is not null)
        {
            try
            {
                await _messageInvoker.SendMessagesAsync(sendRequest, token);
            }
            catch
            {
                var partId = BinaryPrimitives.ReadInt32LittleEndian(sendRequest.Partitioning.Value);
                _logger.LogError(
                    "Error encountered while sending messages - Stream ID:{StreamId}, Topic ID:{TopicId}, Partition ID: {PartitionId}",
                    sendRequest.StreamId, sendRequest.TopicId, partId);
            }

            return;
        }

        await _channel!.Writer.WriteAsync(sendRequest, token);
    }

    public async Task FlushUnsavedBufferAsync(Identifier streamId, Identifier topicId, uint partitionId, bool fsync,
        CancellationToken token = default)
    {
        var url = CreateUrl($"/streams/{streamId}/topics/{topicId}/messages/flush/{partitionId}/{fsync}");

        var response = await _httpClient.GetAsync(url, token);

        if (!response.IsSuccessStatusCode)
        {
            await HandleResponseAsync(response);
        }
    }

    public async Task<PolledMessages> PollMessagesAsync(MessageFetchRequest request,
        Func<byte[], byte[]>? decryptor = null,
        CancellationToken token = default)
    {
        var url = CreateUrl(
            $"/streams/{request.StreamId}/topics/{request.TopicId}/messages?consumer_id={request.Consumer.Id}" +
            $"&partition_id={request.PartitionId}&kind={request.PollingStrategy.Kind}&value={request.PollingStrategy.Value}&count={request.Count}&auto_commit={request.AutoCommit}");

        var response = await _httpClient.GetAsync(url, token);
        if (response.IsSuccessStatusCode)
        {
            var pollMessages = await response.Content.ReadFromJsonAsync<PolledMessages>(_jsonSerializerOptions, token)
                               ?? PolledMessages.Empty;

            if (decryptor is null)
            {
                return pollMessages;
            }

            foreach (var message in pollMessages.Messages)
            {
                message.Payload = decryptor(message.Payload);
            }

            return pollMessages;
        }

        await HandleResponseAsync(response);
        return PolledMessages.Empty;
    }

    public async Task<PolledMessages<TMessage>> PollMessagesAsync<TMessage>(MessageFetchRequest request,
        Func<byte[], TMessage> deserializer, Func<byte[], byte[]>? decryptor = null,
        CancellationToken token = default)
    {
        var url = CreateUrl(
            $"/streams/{request.StreamId}/topics/{request.TopicId}/messages?consumer_id={request.Consumer.Id}" +
            $"&partition_id={request.PartitionId}&kind={request.PollingStrategy.Kind}&value={request.PollingStrategy.Value}&count={request.Count}&auto_commit={request.AutoCommit}");

        var response = await _httpClient.GetAsync(url, token);
        if (response.IsSuccessStatusCode)
        {
            var pollMessages = await response.Content.ReadFromJsonAsync<PolledMessages>(_jsonSerializerOptions, token)
                               ?? PolledMessages.Empty;

            var messages = new List<MessageResponse<TMessage>>();
            foreach (var message in pollMessages.Messages)
            {
                if (decryptor is not null)
                {
                    message.Payload = decryptor(message.Payload);
                }

                messages.Add(new MessageResponse<TMessage>
                {
                    Message = deserializer(message.Payload),
                    Header = message.Header,
                    UserHeaders = message.UserHeaders
                });
            }

            return new PolledMessages<TMessage>
            {
                PartitionId = pollMessages.PartitionId,
                CurrentOffset = pollMessages.CurrentOffset,
                Messages = messages
            };
        }

        await HandleResponseAsync(response);
        return PolledMessages<TMessage>.Empty;
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
                        "Error encountered while saving offset information - Offset: {Offset}, Stream ID: {StreamId}, Topic ID: {TopicId}, Partition ID: {PartitionId}",
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

    public async Task StoreOffsetAsync(Consumer consumer, Identifier streamId, Identifier topicId, ulong offset,
        uint? partitionId, CancellationToken token = default)
    {
        var json = JsonSerializer.Serialize(new StoreOffsetRequest(consumer, partitionId, offset),
            _jsonSerializerOptions);
        var data = new StringContent(json, Encoding.UTF8, "application/json");

        var response
            = await _httpClient.PutAsync($"/streams/{streamId}/topics/{topicId}/consumer-offsets", data, token);
        if (!response.IsSuccessStatusCode)
        {
            await HandleResponseAsync(response);
        }
    }

    public async Task<OffsetResponse?> GetOffsetAsync(Consumer consumer, Identifier streamId, Identifier topicId,
        uint? partitionId, CancellationToken token = default)
    {
        var response = await _httpClient.GetAsync($"/streams/{streamId}/topics/{topicId}/" +
                                                  $"consumer-offsets?consumer_id={consumer.Id}&partition_id={partitionId}",
            token);
        if (response.IsSuccessStatusCode)
        {
            return await response.Content.ReadFromJsonAsync<OffsetResponse>(_jsonSerializerOptions, token);
        }

        await HandleResponseAsync(response);
        return null;
    }

    public async Task DeleteOffsetAsync(Consumer consumer, Identifier streamId, Identifier topicId, uint? partitionId,
        CancellationToken token = default)
    {
        var response = await _httpClient.DeleteAsync(
            $"/streams/{streamId}/topics/{topicId}/consumer-offsets/{consumer}?partition_id={partitionId}", token);
        await HandleResponseAsync(response);
    }

    public async Task<IReadOnlyList<ConsumerGroupResponse>> GetConsumerGroupsAsync(Identifier streamId,
        Identifier topicId, CancellationToken token = default)
    {
        var response = await _httpClient.GetAsync($"/streams/{streamId}/topics/{topicId}/consumer-groups", token);

        if (response.IsSuccessStatusCode)
        {
            return await response.Content.ReadFromJsonAsync<IReadOnlyList<ConsumerGroupResponse>>(
                       _jsonSerializerOptions, token)
                   ?? Array.Empty<ConsumerGroupResponse>();
        }

        await HandleResponseAsync(response);
        return Array.Empty<ConsumerGroupResponse>();
    }

    public async Task<ConsumerGroupResponse?> GetConsumerGroupByIdAsync(Identifier streamId, Identifier topicId,
        Identifier groupId, CancellationToken token = default)
    {
        var response
            = await _httpClient.GetAsync($"/streams/{streamId}/topics/{topicId}/consumer-groups/{groupId}", token);

        if (response.IsSuccessStatusCode)
        {
            return await response.Content.ReadFromJsonAsync<ConsumerGroupResponse>(_jsonSerializerOptions, token);
        }

        await HandleResponseAsync(response);
        return null;
    }

    public async Task<ConsumerGroupResponse?> CreateConsumerGroupAsync(Identifier streamId, Identifier topicId,
        string name, uint? groupId, CancellationToken token = default)
    {
        var json = JsonSerializer.Serialize(new CreateConsumerGroupRequest(name, groupId), _jsonSerializerOptions);

        var data = new StringContent(json, Encoding.UTF8, "application/json");

        var response
            = await _httpClient.PostAsync($"/streams/{streamId}/topics/{topicId}/consumer-groups", data, token);
        if (response.IsSuccessStatusCode)
        {
            return await response.Content.ReadFromJsonAsync<ConsumerGroupResponse>(_jsonSerializerOptions, token);
        }

        await HandleResponseAsync(response);
        return null;
    }

    public async Task DeleteConsumerGroupAsync(Identifier streamId, Identifier topicId, Identifier groupId,
        CancellationToken token = default)
    {
        var response
            = await _httpClient.DeleteAsync($"/streams/{streamId}/topics/{topicId}/consumer-groups/{groupId}", token);
        await HandleResponseAsync(response);
    }

    public Task<ClientResponse?> GetMeAsync(CancellationToken token = default)
    {
        throw new FeatureUnavailableException();
    }

    public async Task<StatsResponse?> GetStatsAsync(CancellationToken token = default)
    {
        var response = await _httpClient.GetAsync("/stats", token);
        if (response.IsSuccessStatusCode)
        {
            return await response.Content.ReadFromJsonAsync<StatsResponse>(_jsonSerializerOptions, token);
        }

        await HandleResponseAsync(response);
        return null;
    }

    public async Task PingAsync(CancellationToken token = default)
    {
        var response = await _httpClient.GetAsync("/ping", token);

        if (!response.IsSuccessStatusCode)
        {
            await HandleResponseAsync(response);
        }
    }

    public async Task<IReadOnlyList<ClientResponse>> GetClientsAsync(CancellationToken token = default)
    {
        var response = await _httpClient.GetAsync("/clients", token);
        if (response.IsSuccessStatusCode)
        {
            return await response.Content.ReadFromJsonAsync<IReadOnlyList<ClientResponse>>(_jsonSerializerOptions,
                       token)
                   ?? Array.Empty<ClientResponse>();
        }

        await HandleResponseAsync(response);
        return Array.Empty<ClientResponse>();
    }

    public async Task<ClientResponse?> GetClientByIdAsync(uint clientId, CancellationToken token = default)
    {
        var response = await _httpClient.GetAsync($"/clients/{clientId}", token);
        if (response.IsSuccessStatusCode)
        {
            return await response.Content.ReadFromJsonAsync<ClientResponse>(_jsonSerializerOptions, token);
        }

        await HandleResponseAsync(response);
        return null;
    }

    [Obsolete("This method is only supported in TCP protocol", true)]
    public Task JoinConsumerGroupAsync(Identifier streamId, Identifier topicId, Identifier groupId,
        CancellationToken token = default)
    {
        throw new FeatureUnavailableException();
    }

    [Obsolete("This method is only supported in TCP protocol", true)]
    public Task LeaveConsumerGroupAsync(Identifier streamId, Identifier topicId, Identifier groupId,
        CancellationToken token = default)
    {
        throw new FeatureUnavailableException();
    }

    public async Task DeletePartitionsAsync(Identifier streamId, Identifier topicId, uint partitionsCount,
        CancellationToken token = default)
    {
        var response
            = await _httpClient.DeleteAsync(
                $"/streams/{streamId}/topics/{topicId}/partitions?partitions_count={partitionsCount}", token);
        if (!response.IsSuccessStatusCode)
        {
            await HandleResponseAsync(response);
        }
    }

    public async Task CreatePartitionsAsync(Identifier streamId, Identifier topicId, uint partitionsCount,
        CancellationToken token = default)
    {
        var json = JsonSerializer.Serialize(new CreatePartitionsRequest(partitionsCount), _jsonSerializerOptions);

        var data = new StringContent(json, Encoding.UTF8, "application/json");

        var response = await _httpClient.PostAsync($"/streams/{streamId}/topics/{topicId}/partitions", data, token);
        if (!response.IsSuccessStatusCode)
        {
            await HandleResponseAsync(response);
        }
    }

    public async Task<UserResponse?> GetUser(Identifier userId, CancellationToken token = default)
    {
        //TODO - this doesn't work prob needs a custom json serializer
        var response = await _httpClient.GetAsync($"/users/{userId}", token);
        if (response.IsSuccessStatusCode)
        {
            return await response.Content.ReadFromJsonAsync<UserResponse>(_jsonSerializerOptions, token);
        }

        await HandleResponseAsync(response);
        return null;
    }

    public async Task<IReadOnlyList<UserResponse>> GetUsers(CancellationToken token = default)
    {
        var response = await _httpClient.GetAsync("/users", token);
        if (response.IsSuccessStatusCode)
        {
            return await response.Content.ReadFromJsonAsync<IReadOnlyList<UserResponse>>(_jsonSerializerOptions, token)
                   ?? Array.Empty<UserResponse>();
        }

        await HandleResponseAsync(response);
        return Array.Empty<UserResponse>();
    }

    public async Task<UserResponse?> CreateUser(string userName, string password, UserStatus status,
        Permissions? permissions = null, CancellationToken token = default)
    {
        var json = JsonSerializer.Serialize(new CreateUserRequest(userName, password, status, permissions),
            _jsonSerializerOptions);

        var content = new StringContent(json, Encoding.UTF8, "application/json");
        var response = await _httpClient.PostAsync("/users", content, token);
        if (response.IsSuccessStatusCode)
        {
            return await response.Content.ReadFromJsonAsync<UserResponse>(_jsonSerializerOptions, token);
        }

        await HandleResponseAsync(response);
        return null;
    }

    public async Task DeleteUser(Identifier userId, CancellationToken token = default)
    {
        var response = await _httpClient.DeleteAsync($"/users/{userId}", token);
        if (!response.IsSuccessStatusCode)
        {
            await HandleResponseAsync(response);
        }
    }

    public async Task UpdateUser(Identifier userId, string? userName = null, UserStatus? status = null,
        CancellationToken token = default)
    {
        var json = JsonSerializer.Serialize(new UpdateUserRequest(userName, status), _jsonSerializerOptions);
        var content = new StringContent(json, Encoding.UTF8, "application/json");
        var response = await _httpClient.PutAsync($"/users/{userId}", content, token);
        if (!response.IsSuccessStatusCode)
        {
            await HandleResponseAsync(response);
        }
    }

    public async Task UpdatePermissions(Identifier userId, Permissions? permissions = null,
        CancellationToken token = default)
    {
        var json = JsonSerializer.Serialize(new UpdateUserPermissionsRequest(permissions), _jsonSerializerOptions);
        var content = new StringContent(json, Encoding.UTF8, "application/json");
        var response = await _httpClient.PutAsync($"/users/{userId}/permissions", content, token);
        if (!response.IsSuccessStatusCode)
        {
            await HandleResponseAsync(response);
        }
    }

    public async Task ChangePassword(Identifier userId, string currentPassword, string newPassword,
        CancellationToken token = default)
    {
        var json = JsonSerializer.Serialize(new ChangePasswordRequest(currentPassword, newPassword),
            _jsonSerializerOptions);
        var content = new StringContent(json, Encoding.UTF8, "application/json");
        var response = await _httpClient.PutAsync($"/users/{userId}/password", content, token);
        if (!response.IsSuccessStatusCode)
        {
            await HandleResponseAsync(response);
        }
    }

    public async Task<AuthResponse?> LoginUser(string userName, string password, CancellationToken token = default)
    {
        // TODO: get version 
        var json = JsonSerializer.Serialize(new LoginUserRequest(userName, password, "", Context),
            _jsonSerializerOptions);

        var data = new StringContent(json, Encoding.UTF8, "application/json");

        var response = await _httpClient.PostAsync("users/login", data, token);
        if (response.IsSuccessStatusCode)
        {
            var authResponse = await response.Content.ReadFromJsonAsync<AuthResponse>(_jsonSerializerOptions, token);
            var jwtToken = authResponse!.AccessToken?.Token;
            if (!string.IsNullOrEmpty(authResponse!.AccessToken!.Token))
            {
                _httpClient.DefaultRequestHeaders.Authorization =
                    new AuthenticationHeaderValue("Bearer", jwtToken);
            }
            else
            {
                throw new Exception("The JWT token is missing.");
            }

            return authResponse;
        }

        await HandleResponseAsync(response);
        return null;
    }

    public async Task LogoutUser(CancellationToken token = default)
    {
        var response = await _httpClient.DeleteAsync("users/logout", token);
        if (!response.IsSuccessStatusCode)
        {
            await HandleResponseAsync(response);
        }

        _httpClient.DefaultRequestHeaders.Authorization = null;
    }

    public async Task<IReadOnlyList<PersonalAccessTokenResponse>> GetPersonalAccessTokensAsync(
        CancellationToken token = default)
    {
        var response = await _httpClient.GetAsync("/personal-access-tokens", token);
        if (response.IsSuccessStatusCode)
        {
            return await response.Content.ReadFromJsonAsync<IReadOnlyList<PersonalAccessTokenResponse>>(
                       _jsonSerializerOptions, token)
                   ?? Array.Empty<PersonalAccessTokenResponse>();
        }

        await HandleResponseAsync(response);
        return Array.Empty<PersonalAccessTokenResponse>();
    }

    public async Task<RawPersonalAccessToken?> CreatePersonalAccessTokenAsync(string name, ulong? expiry = null,
        CancellationToken token = default)
    {
        var json = JsonSerializer.Serialize(new CreatePersonalAccessTokenRequest(name, expiry), _jsonSerializerOptions);

        var content = new StringContent(json, Encoding.UTF8, "application/json");
        var response = await _httpClient.PostAsync("/personal-access-tokens", content, token);
        if (!response.IsSuccessStatusCode)
        {
            await HandleResponseAsync(response);
        }

        return await response.Content.ReadFromJsonAsync<RawPersonalAccessToken>(_jsonSerializerOptions, token);
    }

    public async Task DeletePersonalAccessTokenAsync(string name, CancellationToken token = default)
    {
        var response = await _httpClient.DeleteAsync($"/personal-access-tokens/{name}", token);
        if (!response.IsSuccessStatusCode)
        {
            await HandleResponseAsync(response);
        }
    }

    public async Task<AuthResponse?> LoginWithPersonalAccessToken(string token, CancellationToken ct = default)
    {
        var json = JsonSerializer.Serialize(new LoginWithPersonalAccessTokenRequest(token), _jsonSerializerOptions);

        var content = new StringContent(json, Encoding.UTF8, "application/json");
        var response = await _httpClient.PostAsync("/personal-access-tokens/login", content, ct);
        if (response.IsSuccessStatusCode)
        {
            var authResponse = await response.Content.ReadFromJsonAsync<AuthResponse>(_jsonSerializerOptions, ct);
            var jwtToken = authResponse!.AccessToken?.Token;
            if (!string.IsNullOrEmpty(authResponse!.AccessToken!.Token))
            {
                _httpClient.DefaultRequestHeaders.Authorization =
                    new AuthenticationHeaderValue("Bearer", jwtToken);
            }
            else
            {
                throw new Exception("The JWT token is missing.");
            }

            return authResponse;
        }

        await HandleResponseAsync(response);

        return null;
    }

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
            catch (Exception e)
            {
                _logger.LogError(e,
                    "Error encountered while polling messages - Stream ID: {StreamId}, Topic ID: {TopicId}, Partition ID: {PartitionId}",
                    request.StreamId, request.TopicId, request.PartitionId);
            }
        }

        writer.Complete();
    }

    private static async Task HandleResponseAsync(HttpResponseMessage response)
    {
        if ((int)response.StatusCode > 300 && (int)response.StatusCode < 500)
        {
            var err = await response.Content.ReadAsStringAsync();
            throw new InvalidResponseException(err);
        }

        if (response.StatusCode == HttpStatusCode.InternalServerError)
        {
            throw new Exception("Internal server error");
        }
    }

    private static string CreateUrl(ref MessageRequestInterpolationHandler message)
    {
        return message.ToString();
    }
}