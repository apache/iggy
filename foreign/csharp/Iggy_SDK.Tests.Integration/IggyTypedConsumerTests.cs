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
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Kinds;
using Apache.Iggy.Messages;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Microsoft.Extensions.Logging.Abstractions;
using Shouldly;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.Tests.Integrations;

public class IggyTypedConsumerTests
{
    [ClassDataSource<IggyServerFixture>(Shared = SharedType.PerAssembly)]
    public required IggyServerFixture Fixture { get; init; }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task ReceiveDeserializedAsync_Should_YieldMessages_WithCorrectData(Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var testStream = await CreateTestStreamWithMessages(client, protocol);

        var consumerId = protocol == Protocol.Tcp ? 200 : 300;
        IggyConsumer<string> consumer = BuildTypedConsumer(client, testStream, Consumer.New(consumerId),
            AutoCommitMode.Disabled, new Utf8StringDeserializer());

        await consumer.InitAsync();

        var received = new List<ReceivedMessage<string>>();
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        await foreach (ReceivedMessage<string> msg in consumer.ReceiveDeserializedAsync(cts.Token))
        {
            msg.ShouldNotBeNull();
            msg.Status.ShouldBe(MessageStatus.Success);
            msg.Data.ShouldNotBeNull();
            msg.Data.ShouldStartWith("Test message");
            msg.PartitionId.ShouldBe(1u);
            msg.Error.ShouldBeNull();
            received.Add(msg);
            if (received.Count >= 10)
            {
                break;
            }
        }

        received.Count.ShouldBeGreaterThanOrEqualTo(10);
        await consumer.DisposeAsync();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task ReceiveDeserializedAsync_WithoutInit_Should_Throw_ConsumerNotInitializedException(
        Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var testStream = await CreateTestStreamWithMessages(client, protocol);

        var consumerId = protocol == Protocol.Tcp ? 201 : 301;
        IggyConsumer<string> consumer = BuildTypedConsumer(client, testStream, Consumer.New(consumerId),
            AutoCommitMode.Disabled, new Utf8StringDeserializer());

        await Should.ThrowAsync<ConsumerNotInitializedException>(async () =>
        {
            await foreach (ReceivedMessage<string> _ in consumer.ReceiveDeserializedAsync())
            {
            }
        });

        await consumer.DisposeAsync();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task ReceiveDeserializedAsync_WithAutoCommitAfterReceive_Should_StoreOffset(Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var testStream = await CreateTestStreamWithMessages(client, protocol);

        var consumerId = protocol == Protocol.Tcp ? 202 : 302;
        IggyConsumer<string> consumer = BuildTypedConsumer(client, testStream, Consumer.New(consumerId),
            AutoCommitMode.AfterReceive, new Utf8StringDeserializer(),
            PollingStrategy.First());

        await consumer.InitAsync();

        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var count = 0;

        await foreach (ReceivedMessage<string> _ in consumer.ReceiveDeserializedAsync(cts.Token))
        {
            count++;
            if (count >= 5)
            {
                break;
            }
        }

        await consumer.DisposeAsync();

        var offset = await client.GetOffsetAsync(Consumer.New(consumerId),
            Identifier.String(testStream.StreamId),
            Identifier.String(testStream.TopicId),
            1u);

        offset.ShouldNotBeNull();
        offset.StoredOffset.ShouldBe(3ul);
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task ReceiveDeserializedAsync_WithFailingDeserializer_Should_YieldDeserializationFailed(
        Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var testStream = await CreateTestStreamWithMessages(client, protocol);

        var consumerId = protocol == Protocol.Tcp ? 203 : 303;
        IggyConsumer<string> consumer = BuildTypedConsumer(client, testStream, Consumer.New(consumerId),
            AutoCommitMode.Disabled, new FailingDeserializer());

        await consumer.InitAsync();

        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        await foreach (ReceivedMessage<string> msg in consumer.ReceiveDeserializedAsync(cts.Token))
        {
            msg.Status.ShouldBe(MessageStatus.DeserializationFailed);
            msg.Data.ShouldBeNull();
            msg.Error.ShouldNotBeNull();
            msg.Error.ShouldBeOfType<InvalidOperationException>();
            break;
        }

        await consumer.DisposeAsync();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task ReceiveDeserializedAsync_Should_StopCleanly_OnCancellation(Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var testStream = await CreateTestStreamWithMessages(client, protocol);

        var consumerId = protocol == Protocol.Tcp ? 204 : 304;
        IggyConsumer<string> consumer = BuildTypedConsumer(client, testStream, Consumer.New(consumerId),
            AutoCommitMode.Disabled, new Utf8StringDeserializer());

        await consumer.InitAsync();

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));

        await Should.NotThrowAsync(async () =>
        {
            try
            {
                await foreach (ReceivedMessage<string> _ in consumer.ReceiveDeserializedAsync(cts.Token))
                {
                }
            }
            catch (OperationCanceledException)
            {
            }
        });

        await consumer.DisposeAsync();
    }

    private static IggyConsumer<string> BuildTypedConsumer(IIggyClient client,
        TestStreamInfo stream,
        Consumer consumer,
        AutoCommitMode autoCommitMode,
        IDeserializer<string> deserializer,
        PollingStrategy? pollingStrategy = null)
    {
        var config = new IggyConsumerConfig<string>
        {
            StreamId = Identifier.String(stream.StreamId),
            TopicId = Identifier.String(stream.TopicId),
            Consumer = consumer,
            PollingStrategy = pollingStrategy ?? PollingStrategy.Next(),
            BatchSize = 10,
            PartitionId = 1,
            AutoCommitMode = autoCommitMode,
            AutoCommit = autoCommitMode != AutoCommitMode.Disabled,
            PollingIntervalMs = 0,
            Deserializer = deserializer
        };
        return new IggyConsumer<string>(client, config, NullLoggerFactory.Instance);
    }

    private async Task<TestStreamInfo> CreateTestStreamWithMessages(IIggyClient client, Protocol protocol,
        uint partitionsCount = 5, int messagesPerPartition = 100)
    {
        var streamId = $"typed_stream_{Guid.NewGuid()}_{protocol.ToString().ToLowerInvariant()}";
        var topicId = "test_topic";

        await client.CreateStreamAsync(streamId);
        await client.CreateTopicAsync(Identifier.String(streamId), topicId, partitionsCount);

        for (uint partitionId = 0; partitionId < partitionsCount; partitionId++)
        {
            var messages = new List<Message>();
            for (var i = 0; i < messagesPerPartition; i++)
            {
                messages.Add(new Message(Guid.NewGuid(),
                    Encoding.UTF8.GetBytes($"Test message {i} for partition {partitionId}")));
            }

            await client.SendMessagesAsync(Identifier.String(streamId),
                Identifier.String(topicId),
                Partitioning.PartitionId((int)partitionId),
                messages);
        }

        return new TestStreamInfo(streamId, topicId, partitionsCount, messagesPerPartition);
    }

    private record TestStreamInfo(string StreamId, string TopicId, uint PartitionsCount, int MessagesPerPartition);

    private sealed class Utf8StringDeserializer : IDeserializer<string>
    {
        public string Deserialize(ReadOnlyMemory<byte> data)
        {
            return Encoding.UTF8.GetString(data.Span);
        }
    }

    private sealed class FailingDeserializer : IDeserializer<string>
    {
        public string Deserialize(ReadOnlyMemory<byte> data)
        {
            throw new InvalidOperationException("Intentional deserialization failure");
        }
    }
}
