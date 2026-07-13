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
using System.Text;
using Apache.Iggy.Configuration;
using Apache.Iggy.Encryption;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.IggyClient.Implementations;
using Apache.Iggy.Kinds;
using Microsoft.Extensions.Logging.Abstractions;

namespace Apache.Iggy.Tests.ClientTests;

public class RawClientEncryptionTests
{
    private const string EmptyPollResponse = "{\"partition_id\":0,\"current_offset\":0,\"messages\":[]}";
    private static readonly Identifier StreamId = Identifier.Numeric(1);
    private static readonly Identifier TopicId = Identifier.Numeric(1);

    [Fact]
    public async Task PollMessages_WithEncryptorAndAutoCommit_Throws_ByDefault()
    {
        var client = CreateHttpClient(allowAutoCommitWithEncryptor: false);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            client.PollMessagesAsync(StreamId, TopicId, 0, Consumer.New(1), PollingStrategy.Next(), 10, true,
                TestContext.Current.CancellationToken));

        Assert.Contains("autoCommit", ex.Message);
    }

    [Fact]
    public async Task PollMessages_WithEncryptorAndAutoCommit_IsAllowed_WhenOptedIn()
    {
        var client = CreateHttpClient(allowAutoCommitWithEncryptor: true);

        var polled = await client.PollMessagesAsync(StreamId, TopicId, 0, Consumer.New(1), PollingStrategy.Next(), 10,
            true, TestContext.Current.CancellationToken);

        Assert.Empty(polled.Messages);
    }

    [Fact]
    public async Task PollMessages_WithoutAutoCommit_Succeeds_WithEncryptorRegardlessOfOptIn()
    {
        var client = CreateHttpClient(allowAutoCommitWithEncryptor: false);

        var polled = await client.PollMessagesAsync(StreamId, TopicId, 0, Consumer.New(1), PollingStrategy.Next(), 10,
            false, TestContext.Current.CancellationToken);

        Assert.Empty(polled.Messages);
    }

    [Fact]
    public async Task TcpPollMessages_WithEncryptorAndAutoCommit_Throws_ByDefault()
    {
        var configuration = new IggyClientConfigurator
        {
            BaseAddress = "127.0.0.1:8090",
            Protocol = Protocol.Tcp,
            MessageEncryptor = new AesMessageEncryptor(AesMessageEncryptor.GenerateKey())
        };
        using var client = new TcpMessageStream(configuration, NullLoggerFactory.Instance);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            client.PollMessagesAsync(StreamId, TopicId, 0, Consumer.New(1), PollingStrategy.Next(), 10, true,
                TestContext.Current.CancellationToken));

        Assert.Contains("autoCommit", ex.Message);
    }

    [Fact]
    public async Task PollMessages_WithEncryptor_UndecryptablePayload_ThrowsMessageDecryptionException()
    {
        var garbage = new byte[44];
        Random.Shared.NextBytes(garbage);
        var pollResponse = "{\"partition_id\":3,\"current_offset\":42,\"messages\":[{" +
                           "\"header\":{\"checksum\":0,\"id\":1,\"offset\":42,\"timestamp\":0," +
                           "\"origin_timestamp\":0,\"user_headers_length\":0,\"payload_length\":44,\"reserved\":0}," +
                           $"\"payload\":\"{Convert.ToBase64String(garbage)}\"}}]}}";
        var client = CreateHttpClient(allowAutoCommitWithEncryptor: false, pollResponse);

        var ex = await Assert.ThrowsAsync<MessageDecryptionException>(() =>
            client.PollMessagesAsync(StreamId, TopicId, 0, Consumer.New(1), PollingStrategy.Next(), 10, false,
                TestContext.Current.CancellationToken));

        Assert.Equal(42ul, ex.Offset);
        Assert.Equal(3u, ex.PartitionId);
    }

    private static HttpMessageStream CreateHttpClient(bool allowAutoCommitWithEncryptor,
        string pollResponse = EmptyPollResponse)
    {
        var httpClient = new HttpClient(new StubHandler(pollResponse))
        {
            BaseAddress = new Uri("http://localhost")
        };
        var encryptor = new AesMessageEncryptor(AesMessageEncryptor.GenerateKey());
        return new HttpMessageStream(httpClient, encryptor, allowAutoCommitWithEncryptor);
    }

    private sealed class StubHandler(string json) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(json, Encoding.UTF8, "application/json")
            });
        }
    }
}
