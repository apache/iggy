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

using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Apache.Iggy.Contracts;
using Apache.Iggy.Contracts.Auth;
using Apache.Iggy.Contracts.Http;
using Apache.Iggy.Contracts.Http.Auth;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Kinds;
using Apache.Iggy.Messages;
using Apache.Iggy.StringHandlers;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.IggyClient.Implementations;

/// <summary>
///     Implementation of <see cref="IIggyClient" /> that uses <see cref="HttpClient" /> to communicate with the server.
/// </summary>
public class HttpMessageStream : IIggyClient
{
    private const string Context = "csharp-sdk";

    private readonly HttpClient _httpClient;

    //TODO - create mechanism for refreshing jwt token
    //TODO - replace the HttpClient with IHttpClientFactory, when implementing support for ASP.NET Core DI
    //TODO - the error handling pattern is pretty ugly, look into moving it into an extension method
    private readonly JsonSerializerOptions _jsonSerializerOptions;

    internal HttpMessageStream(HttpClient httpClient)
    {
        _httpClient = httpClient;

        _jsonSerializerOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
            Converters = { new JsonStringEnumConverter(JsonNamingPolicy.SnakeCaseLower) }
        };
    }

    /// <inheritdoc />
    public async Task<StreamResponse?> CreateStreamAsync(string name, CancellationToken token = default)
    {
        var json = JsonSerializer.Serialize(new CreateStreamRequest(name), _jsonSerializerOptions);

        var data = new StringContent(json, Encoding.UTF8, "application/json");

        var response = await _httpClient.PostAsync("/streams", data, token);

        if (response.IsSuccessStatusCode)
        {
            return await response.Content.ReadFromJsonAsync<StreamResponse>(_jsonSerializerOptions, token);
        }

        await HandleResponseAsync(response);

        return null;
    }

    /// <inheritdoc />
    public async Task PurgeStreamAsync(Identifier streamId, CancellationToken token = default)
    {
        var response = await _httpClient.DeleteAsync($"/streams/{streamId}/purge", token);
        if (!response.IsSuccessStatusCode)
        {
            await HandleResponseAsync(response);
        }
    }

    /// <inheritdoc />
    public async Task DeleteStreamAsync(Identifier streamId, CancellationToken token = default)
    {
        var response = await _httpClient.DeleteAsync($"/streams/{streamId}", token);
        if (!response.IsSuccessStatusCode)
        {
            await HandleResponseAsync(response);
        }
    }

    /// <inheritdoc />
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

    /// <inheritdoc />
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

    /// <inheritdoc />
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

    /// <inheritdoc />
    public async Task<TopicResponse?> CreateTopicAsync(Identifier streamId, string name, uint partitionsCount,
        CompressionAlgorithm compressionAlgorithm = CompressionAlgorithm.None, byte? replicationFactor = null,
        ulong messageExpiry = 0, ulong maxTopicSize = 0,
        CancellationToken token = default)
    {
        var json = JsonSerializer.Serialize(new CreateTopicRequest
        {
            Name = name,
            CompressionAlgorithm = compressionAlgorithm,
            MaxTopicSize = maxTopicSize,
            MessageExpiry = messageExpiry,
            PartitionsCount = partitionsCount,
            ReplicationFactor = replicationFactor
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

    /// <inheritdoc />
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

    /// <inheritdoc />
    public async Task DeleteTopicAsync(Identifier streamId, Identifier topicId, CancellationToken token = default)
    {
        var response = await _httpClient.DeleteAsync($"/streams/{streamId}/topics/{topicId}", token);
        if (!response.IsSuccessStatusCode)
        {
            await HandleResponseAsync(response);
        }
    }

    /// <inheritdoc />
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

    /// <inheritdoc />
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

    /// <inheritdoc />
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

    /// <inheritdoc />
    public async Task SendMessagesAsync(Identifier streamId, Identifier topicId, Partitioning partitioning,
        IList<Message> messages,
        CancellationToken token = default)
    {
        var request = new MessageSendRequest
        {
            StreamId = streamId,
            TopicId = topicId,
            Partitioning = partitioning,
            Messages = messages
        };
        var json = JsonSerializer.Serialize(request, _jsonSerializerOptions);
        var data = new StringContent(json, Encoding.UTF8, "application/json");

        var response = await _httpClient.PostAsync($"/streams/{request.StreamId}/topics/{request.TopicId}/messages",
            data,
            token);

        if (!response.IsSuccessStatusCode)
        {
            await HandleResponseAsync(response);
        }
    }

    /// <inheritdoc />
    public async Task FlushUnsavedBufferAsync(Identifier streamId, Identifier topicId, uint partitionId, bool fsync,
        CancellationToken token = default)
    {
        var url = CreateUrl($"/streams/{streamId}/topics/{topicId}/messages/flush/{partitionId}/{fsync}");

        var response = await _httpClient.GetAsync(url, token);

        if (!response.IsSuccessStatusCode)
        {
            await HandleResponseAsync(response, true);
        }
    }

    /// <inheritdoc />
    public async Task<PolledMessages> PollMessagesAsync(Identifier streamId, Identifier topicId, uint? partitionId,
        Consumer consumer,
        PollingStrategy pollingStrategy, uint count, bool autoCommit, CancellationToken token = default)
    {
        var partitionIdParam = partitionId.HasValue ? $"&partition_id={partitionId.Value}" : string.Empty;
        var url = CreateUrl($"/streams/{streamId}/topics/{topicId}/messages?consumer_id={consumer.ConsumerId}" +
                            $"{partitionIdParam}&kind={pollingStrategy.Kind}&value={pollingStrategy.Value}&count={count}&auto_commit={autoCommit}");

        var response = await _httpClient.GetAsync(url, token);
        if (response.IsSuccessStatusCode)
        {
            var pollMessages = await response.Content.ReadFromJsonAsync<PolledMessages>(_jsonSerializerOptions, token)
                               ?? PolledMessages.Empty;

            return pollMessages;
        }

        await HandleResponseAsync(response, true);
        return PolledMessages.Empty;
    }

    /// <inheritdoc />
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

    /// <inheritdoc />
    public async Task<OffsetResponse?> GetOffsetAsync(Consumer consumer, Identifier streamId, Identifier topicId,
        uint? partitionId, CancellationToken token = default)
    {
        var partitionIdParam = partitionId.HasValue ? $"&partition_id={partitionId.Value}" : string.Empty;
        var response = await _httpClient.GetAsync($"/streams/{streamId}/topics/{topicId}/" +
                                                  $"consumer-offsets?consumer_id={consumer.ConsumerId}{partitionIdParam}",
            token);
        if (response.IsSuccessStatusCode)
        {
            return await response.Content.ReadFromJsonAsync<OffsetResponse>(_jsonSerializerOptions, token);
        }

        await HandleResponseAsync(response);
        return null;
    }

    /// <inheritdoc />
    public async Task DeleteOffsetAsync(Consumer consumer, Identifier streamId, Identifier topicId, uint? partitionId,
        CancellationToken token = default)
    {
        var partitionIdParam = partitionId.HasValue ? $"?partition_id={partitionId.Value}" : string.Empty;
        var response = await _httpClient.DeleteAsync(
            $"/streams/{streamId}/topics/{topicId}/consumer-offsets/{consumer}{partitionIdParam}", token);
        await HandleResponseAsync(response);
    }

    /// <inheritdoc />
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

    /// <inheritdoc />
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

    /// <inheritdoc />
    public async Task<ConsumerGroupResponse?> CreateConsumerGroupAsync(Identifier streamId, Identifier topicId,
        string name, CancellationToken token = default)
    {
        var json = JsonSerializer.Serialize(new CreateConsumerGroupRequest(name), _jsonSerializerOptions);

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

    /// <inheritdoc />
    public async Task DeleteConsumerGroupAsync(Identifier streamId, Identifier topicId, Identifier groupId,
        CancellationToken token = default)
    {
        var response
            = await _httpClient.DeleteAsync($"/streams/{streamId}/topics/{topicId}/consumer-groups/{groupId}", token);
        await HandleResponseAsync(response);
    }

    /// <summary>
    ///     This method is only supported in TCP protocol
    /// </summary>
    /// <param name="token"></param>
    /// <returns></returns>
    /// <exception cref="FeatureUnavailableException"></exception>
    public Task<ClientResponse?> GetMeAsync(CancellationToken token = default)
    {
        throw new FeatureUnavailableException();
    }

    /// <inheritdoc />
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

    /// <inheritdoc />
    public async Task<ClusterMetadata?> GetClusterMetadataAsync(CancellationToken token = default)
    {
        var response = await _httpClient.GetAsync("/cluster/metadata", token);
        if (response.IsSuccessStatusCode)
        {
            return await response.Content.ReadFromJsonAsync<ClusterMetadata>(_jsonSerializerOptions, token);
        }

        await HandleResponseAsync(response);

        return null;
    }

    /// <inheritdoc />
    public async Task PingAsync(CancellationToken token = default)
    {
        var response = await _httpClient.GetAsync("/ping", token);

        if (!response.IsSuccessStatusCode)
        {
            await HandleResponseAsync(response);
        }
    }

    /// <inheritdoc />
    public Task ConnectAsync(CancellationToken token = default)
    {
        return Task.CompletedTask;
    }

    /// <inheritdoc />
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

    /// <inheritdoc />
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

    /// <summary>
    ///     This method is only supported in TCP protocol
    /// </summary>
    /// <param name="streamId">The identifier of the stream containing the topic (numeric ID or name).</param>
    /// <param name="topicId">The identifier of the topic (numeric ID or name).</param>
    /// <param name="groupId">The identifier of the consumer group to join (numeric ID or name).</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="FeatureUnavailableException"></exception>
    [Obsolete("This method is only supported in TCP protocol", true)]
    public Task JoinConsumerGroupAsync(Identifier streamId, Identifier topicId, Identifier groupId,
        CancellationToken token = default)
    {
        throw new FeatureUnavailableException();
    }

    /// <summary>
    ///     This method is only supported in TCP protocol
    /// </summary>
    /// <param name="streamId">The identifier of the stream containing the topic (numeric ID or name).</param>
    /// <param name="topicId">The identifier of the topic (numeric ID or name).</param>
    /// <param name="groupId">The identifier of the consumer group to leave (numeric ID or name).</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="FeatureUnavailableException"></exception>
    [Obsolete("This method is only supported in TCP protocol", true)]
    public Task LeaveConsumerGroupAsync(Identifier streamId, Identifier topicId, Identifier groupId,
        CancellationToken token = default)
    {
        throw new FeatureUnavailableException();
    }

    /// <inheritdoc />
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

    /// <inheritdoc />
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

    /// <inheritdoc />
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

    /// <inheritdoc />
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

    /// <inheritdoc />
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

    /// <inheritdoc />
    public async Task DeleteUser(Identifier userId, CancellationToken token = default)
    {
        var response = await _httpClient.DeleteAsync($"/users/{userId}", token);
        if (!response.IsSuccessStatusCode)
        {
            await HandleResponseAsync(response);
        }
    }

    /// <inheritdoc />
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

    /// <inheritdoc />
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

    /// <inheritdoc />
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

    /// <inheritdoc />
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

    /// <inheritdoc />
    public async Task LogoutUser(CancellationToken token = default)
    {
        var response = await _httpClient.DeleteAsync("users/logout", token);
        if (!response.IsSuccessStatusCode)
        {
            await HandleResponseAsync(response);
        }

        _httpClient.DefaultRequestHeaders.Authorization = null;
    }

    /// <inheritdoc />
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

    /// <inheritdoc />
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

    /// <inheritdoc />
    public async Task DeletePersonalAccessTokenAsync(string name, CancellationToken token = default)
    {
        var response = await _httpClient.DeleteAsync($"/personal-access-tokens/{name}", token);
        if (!response.IsSuccessStatusCode)
        {
            await HandleResponseAsync(response);
        }
    }

    /// <inheritdoc />
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

    /// <summary>
    ///     Dispose the client.
    /// </summary>
    public void Dispose()
    {
    }

    /// <inheritdoc />
    public void SubscribeConnectionEvents(Func<ConnectionStateChangedEventArgs, Task> callback)
    {
    }

    /// <inheritdoc />
    public void UnsubscribeConnectionEvents(Func<ConnectionStateChangedEventArgs, Task> callback)
    {
    }

    private static async Task HandleResponseAsync(HttpResponseMessage response, bool shouldThrowOnGetNotFound = false)
    {
        if ((int)response.StatusCode > 300
            && (int)response.StatusCode < 500
            && !(response.RequestMessage!.Method == HttpMethod.Get && response.StatusCode == HttpStatusCode.NotFound &&
                 !shouldThrowOnGetNotFound))
        {
            var err = await response.Content.ReadAsStringAsync();
            var errorModel = JsonSerializer.Deserialize<ErrorResponse>(err);
            throw new IggyInvalidStatusCodeException(errorModel?.Id ?? -1, err);
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
