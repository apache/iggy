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

using System.Text;
using Apache.Iggy.Consumers;
using Apache.Iggy.Encryption;
using Apache.Iggy.Enums;
using Apache.Iggy.Headers;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Kinds;
using Apache.Iggy.Mappers;
using Apache.Iggy.Messages;
using Apache.Iggy.Publishers;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Shouldly;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.Tests.Integrations;

public class HeaderEncryptionIntegrationTests
{
    [ClassDataSource<IggyServerFixture>(Shared = SharedType.PerAssembly)]
    public required IggyServerFixture Fixture { get; init; }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task SendMessages_WithEncryptedHeaders_Should_NotBeReadableWithoutDecryptor(Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var encryptor = CreateEncryptor();
        var testStream = await CreateTestStream(client, protocol);
        var streamId = Identifier.String(testStream.StreamId);
        var topicId = Identifier.String(testStream.TopicId);

        // Send message with encrypted headers via publisher
        var publisher = IggyPublisherBuilder
            .Create(client, streamId, topicId)
            .WithPartitioning(Partitioning.PartitionId(0))
            .WithEncryptor(encryptor)
            .Build();

        await publisher.InitAsync();

        var headers = CreateTestHeaders();
        var messages = new List<Message>
        {
            new(Guid.NewGuid(), Encoding.UTF8.GetBytes("encrypted payload"), headers)
        };

        await publisher.SendMessagesAsync(messages);
        await publisher.DisposeAsync();

        // Poll with a normal client (no decryptor)
        var polled = await client.PollMessagesAsync(
            streamId, topicId, 0,
            Consumer.New(0),
            PollingStrategy.Next(),
            1, false);

        polled.Messages.Count.ShouldBe(1);
        var msg = polled.Messages[0];

        // Payload should be encrypted (not readable as plaintext)
        Encoding.UTF8.GetString(msg.Payload).ShouldNotBe("encrypted payload");

        // Extract encrypted header bytes:
        // TCP path: RawUserHeaders is set, UserHeaders is null
        // HTTP path: UserHeaders contains sentinel key with encrypted bytes
        byte[]? encryptedHeaderBytes = msg.RawUserHeaders;
        if (encryptedHeaderBytes is null
            && msg.UserHeaders is { Count: 1 }
            && msg.UserHeaders.TryGetValue(HeaderKey.EncryptedHeadersSentinel, out var sentinelValue))
        {
            encryptedHeaderBytes = sentinelValue.Value;
        }

        encryptedHeaderBytes.ShouldNotBeNull();
        encryptedHeaderBytes!.Length.ShouldBeGreaterThan(0);

        // Manually decrypt and verify payload
        var decryptedPayload = encryptor.Decrypt(msg.Payload);
        Encoding.UTF8.GetString(decryptedPayload).ShouldBe("encrypted payload");

        // Manually decrypt and verify headers
        var decryptedHeaderBytesResult = encryptor.Decrypt(encryptedHeaderBytes);
        var decryptedHeaders = BinaryMapper.MapHeaders(decryptedHeaderBytesResult);
        decryptedHeaders.Count.ShouldBe(3);

        var typeHeader = decryptedHeaders[new HeaderKey { Kind = HeaderKind.String, Value = "type"u8.ToArray() }];
        Encoding.UTF8.GetString(typeHeader.Value).ShouldBe("test-message");
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task ReceiveAsync_WithDecryptor_Should_DecryptHeadersCorrectly(Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var encryptor = CreateEncryptor();
        var testStream = await CreateTestStream(client, protocol);
        var streamId = Identifier.String(testStream.StreamId);
        var topicId = Identifier.String(testStream.TopicId);

        // Send encrypted message via publisher
        var publisher = IggyPublisherBuilder
            .Create(client, streamId, topicId)
            .WithPartitioning(Partitioning.PartitionId(0))
            .WithEncryptor(encryptor)
            .Build();

        await publisher.InitAsync();

        var headers = CreateTestHeaders();
        var messages = new List<Message>
        {
            new(Guid.NewGuid(), Encoding.UTF8.GetBytes("consumer test payload"), headers)
        };

        await publisher.SendMessagesAsync(messages);
        await publisher.DisposeAsync();

        // Poll with IggyConsumer that has decryptor configured
        var consumer = IggyConsumerBuilder
            .Create(client, streamId, topicId, Consumer.New(1))
            .WithPollingStrategy(PollingStrategy.Next())
            .WithBatchSize(1)
            .WithPartitionId(0)
            .WithAutoCommitMode(AutoCommitMode.Disabled)
            .WithDecryptor(encryptor)
            .Build();

        await consumer.InitAsync();

        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        ReceivedMessage? received = null;

        await foreach (var msg in consumer.ReceiveAsync(cts.Token))
        {
            received = msg;
            break;
        }

        await consumer.DisposeAsync();

        // Message should be successfully decrypted
        received.ShouldNotBeNull();
        received!.Status.ShouldBe(MessageStatus.Success);

        // Payload should be decrypted
        Encoding.UTF8.GetString(received.Message.Payload).ShouldBe("consumer test payload");

        // Headers should be decrypted and parsed
        received.Message.UserHeaders.ShouldNotBeNull();
        received.Message.UserHeaders!.Count.ShouldBe(3);

        var batchHeader = received.Message.UserHeaders[new HeaderKey { Kind = HeaderKind.String, Value = "batch"u8.ToArray() }];
        BitConverter.ToUInt64(batchHeader.Value).ShouldBe(1UL);

        var typeHeader = received.Message.UserHeaders[new HeaderKey { Kind = HeaderKind.String, Value = "type"u8.ToArray() }];
        Encoding.UTF8.GetString(typeHeader.Value).ShouldBe("test-message");

        var encHeader = received.Message.UserHeaders[new HeaderKey { Kind = HeaderKind.String, Value = "encrypted"u8.ToArray() }];
        encHeader.Value[0].ShouldBe((byte)1);
    }

    private static AesMessageEncryptor CreateEncryptor()
    {
        return new AesMessageEncryptor(AesMessageEncryptor.GenerateKey());
    }

    private static Dictionary<HeaderKey, HeaderValue> CreateTestHeaders()
    {
        return new Dictionary<HeaderKey, HeaderValue>
        {
            {
                new HeaderKey { Kind = HeaderKind.String, Value = "batch"u8.ToArray() },
                new HeaderValue { Kind = HeaderKind.Uint64, Value = BitConverter.GetBytes(1UL) }
            },
            {
                new HeaderKey { Kind = HeaderKind.String, Value = "type"u8.ToArray() },
                new HeaderValue { Kind = HeaderKind.String, Value = "test-message"u8.ToArray() }
            },
            {
                new HeaderKey { Kind = HeaderKind.String, Value = "encrypted"u8.ToArray() },
                new HeaderValue { Kind = HeaderKind.Bool, Value = [1] }
            },
        };
    }

    private async Task<TestStreamInfo> CreateTestStream(IIggyClient client, Protocol protocol)
    {
        var streamId = $"enc_stream_{Guid.NewGuid()}_{protocol.ToString().ToLowerInvariant()}";
        var topicId = "enc_topic";

        await client.CreateStreamAsync(streamId);
        await client.CreateTopicAsync(Identifier.String(streamId), topicId, 1);

        return new TestStreamInfo(streamId, topicId);
    }

    private record TestStreamInfo(string StreamId, string TopicId);
}
