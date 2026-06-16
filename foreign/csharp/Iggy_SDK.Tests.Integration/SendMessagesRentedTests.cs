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
using Apache.Iggy.Contracts;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Extensions;
using Apache.Iggy.Headers;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Kinds;
using Apache.Iggy.Messages;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Shouldly;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.Tests.Integrations;

public class SendMessagesRentedTests
{
    private const string TopicName = "test-topic";

    [ClassDataSource<IggyServerFixture>(Shared = SharedType.PerAssembly)]
    public required IggyServerFixture Fixture { get; init; }

    private async Task<(IIggyClient client, string streamName)> CreateStreamAndTopic(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var streamName = $"send-rented-{Guid.NewGuid():N}";

        await client.CreateStreamAsync(streamName);
        await client.CreateTopicAsync(Identifier.String(streamName), TopicName, 1);

        return (client, streamName);
    }

    private static async Task<IReadOnlyList<MessageResponse>> PollAll(IIggyClient client, string streamName, int count)
    {
        var response = await client.PollMessagesAsync(new MessageFetchRequest
        {
            Count = (uint)count,
            AutoCommit = true,
            Consumer = Consumer.New(1),
            PartitionId = 0,
            PollingStrategy = PollingStrategy.Next(),
            StreamId = Identifier.String(streamName),
            TopicId = Identifier.String(TopicName)
        });

        return response.Messages;
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task SendRentedMessage_Single_Should_RoundTrip(Protocol protocol)
    {
        var (client, streamName) = await CreateStreamAndTopic(protocol);

        var id = Guid.NewGuid();
        var payload = Encoding.UTF8.GetBytes("single-rented-payload");

        using (var builder = new RentedMessageBatchBuilder())
        {
            builder.Add(payload, id);
            using var batch = builder.Build();
            await client.SendMessagesAsync(Identifier.String(streamName),
                Identifier.String(TopicName), Partitioning.None(), batch.Messages[0]);
        }

        IReadOnlyList<MessageResponse> messages = await PollAll(client, streamName, 1);

        messages.Count.ShouldBe(1);
        messages[0].Header.Id.ShouldBe(id.ToUInt128());
        messages[0].Payload.ShouldBe(payload);
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task SendRentedBatch_NoHeaders_Should_RoundTrip(Protocol protocol)
    {
        var (client, streamName) = await CreateStreamAndTopic(protocol);

        const int count = 10;
        var sent = new (UInt128 Id, byte[] Payload)[count];

        using (var builder = new RentedMessageBatchBuilder())
        {
            for (var i = 0; i < count; i++)
            {
                var id = Guid.NewGuid();
                var payload = Encoding.UTF8.GetBytes($"rented-payload-{i}-{Guid.NewGuid():N}");
                sent[i] = (id.ToUInt128(), payload);
                builder.Add(payload, id);
            }

            using var batch = builder.Build();
            await client.SendMessagesAsync(Identifier.String(streamName),
                Identifier.String(TopicName), Partitioning.None(), batch.Messages);
        }

        IReadOnlyList<MessageResponse> messages = await PollAll(client, streamName, count);

        messages.Count.ShouldBe(count);
        Dictionary<UInt128, byte[]> byId = sent.ToDictionary(s => s.Id, s => s.Payload);
        foreach (var message in messages)
        {
            byId.ShouldContainKey(message.Header.Id);
            message.Payload.ShouldBe(byId[message.Header.Id]);
            message.UserHeaders.ShouldBeNull();
        }
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task SendRentedBatch_WithHeaders_Should_RoundTrip(Protocol protocol)
    {
        var (client, streamName) = await CreateStreamAndTopic(protocol);

        const int count = 5;
        var sent = new (UInt128 Id, byte[] Payload)[count];

        using (var builder = new RentedMessageBatchBuilder())
        {
            for (var i = 0; i < count; i++)
            {
                var id = Guid.NewGuid();
                var payload = Encoding.UTF8.GetBytes($"rented-with-headers-{i}");
                sent[i] = (id.ToUInt128(), payload);
                builder.Add(payload, id,
                    new Dictionary<HeaderKey, HeaderValue>
                    {
                        { HeaderKey.FromString("header1"), HeaderValue.FromString("value1") },
                        { HeaderKey.FromString("header2"), HeaderValue.FromInt32(444) }
                    });
            }

            using var batch = builder.Build();
            await client.SendMessagesAsync(Identifier.String(streamName),
                Identifier.String(TopicName), Partitioning.None(), batch.Messages);
        }

        IReadOnlyList<MessageResponse> messages = await PollAll(client, streamName, count);

        messages.Count.ShouldBe(count);
        Dictionary<UInt128, byte[]> byId = sent.ToDictionary(s => s.Id, s => s.Payload);
        foreach (var message in messages)
        {
            byId.ShouldContainKey(message.Header.Id);
            message.Payload.ShouldBe(byId[message.Header.Id]);
            message.UserHeaders.ShouldNotBeNull();
            message.UserHeaders.Count.ShouldBe(2);
            message.UserHeaders[HeaderKey.FromString("header1")].ToString().ShouldBe("value1");
            message.UserHeaders[HeaderKey.FromString("header2")].ToInt32().ShouldBe(444);
        }
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task SendRentedMessage_CallbackPayload_Should_RoundTrip(Protocol protocol)
    {
        var (client, streamName) = await CreateStreamAndTopic(protocol);

        var id = Guid.NewGuid();
        var text = "callback-serialized-payload";

        using (var builder = new RentedMessageBatchBuilder())
        {
            builder.Add(text, static (state, writer) =>
            {
                var bytes = Encoding.UTF8.GetBytes(state);
                bytes.CopyTo(writer.GetSpan(bytes.Length));
                writer.Advance(bytes.Length);
            }, id);

            using var batch = builder.Build();
            await client.SendMessagesAsync(Identifier.String(streamName),
                Identifier.String(TopicName), Partitioning.None(), batch.Messages[0]);
        }

        IReadOnlyList<MessageResponse> messages = await PollAll(client, streamName, 1);

        messages.Count.ShouldBe(1);
        messages[0].Header.Id.ShouldBe(id.ToUInt128());
        Encoding.UTF8.GetString(messages[0].Payload).ShouldBe(text);
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task SendRentedBatch_Large_Should_RoundTrip(Protocol protocol)
    {
        var (client, streamName) = await CreateStreamAndTopic(protocol);

        const int count = 100;
        var sent = new (UInt128 Id, byte[] Payload)[count];

        // Small size hint forces the shared buffer to grow several times mid-batch.
        using (var builder = new RentedMessageBatchBuilder(16))
        {
            for (var i = 0; i < count; i++)
            {
                var id = Guid.NewGuid();
                var payload = Encoding.UTF8.GetBytes(new string((char)('a' + i % 26), 100 + i));
                sent[i] = (id.ToUInt128(), payload);
                builder.Add(payload, id);
            }

            using var batch = builder.Build();
            await client.SendMessagesAsync(Identifier.String(streamName),
                Identifier.String(TopicName), Partitioning.None(), batch.Messages);
        }

        IReadOnlyList<MessageResponse> messages = await PollAll(client, streamName, count);

        messages.Count.ShouldBe(count);
        Dictionary<UInt128, byte[]> byId = sent.ToDictionary(s => s.Id, s => s.Payload);
        foreach (var message in messages)
        {
            byId.ShouldContainKey(message.Header.Id);
            message.Payload.ShouldBe(byId[message.Header.Id]);
        }
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task SendRentedBatch_MixedAddOverloads_Should_RoundTrip(Protocol protocol)
    {
        var (client, streamName) = await CreateStreamAndTopic(protocol);

        var spanId = Guid.NewGuid();
        var spanPayload = Encoding.UTF8.GetBytes("span-added");
        var callbackId = Guid.NewGuid();
        var callbackText = "callback-added";

        using (var builder = new RentedMessageBatchBuilder())
        {
            builder.Add(spanPayload, spanId);
            builder.Add(callbackText, static (state, writer) =>
            {
                var bytes = Encoding.UTF8.GetBytes(state);
                bytes.CopyTo(writer.GetSpan(bytes.Length));
                writer.Advance(bytes.Length);
            }, callbackId);

            using var batch = builder.Build();
            await client.SendMessagesAsync(Identifier.String(streamName),
                Identifier.String(TopicName), Partitioning.None(), batch.Messages);
        }

        IReadOnlyList<MessageResponse> messages = await PollAll(client, streamName, 2);

        messages.Count.ShouldBe(2);
        Dictionary<UInt128, byte[]> byId = messages.ToDictionary(m => m.Header.Id, m => m.Payload);
        byId[spanId.ToUInt128()].ShouldBe(spanPayload);
        Encoding.UTF8.GetString(byId[callbackId.ToUInt128()]).ShouldBe(callbackText);
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task SendRentedBatch_WithoutExplicitIds_Should_RoundTrip(Protocol protocol)
    {
        var (client, streamName) = await CreateStreamAndTopic(protocol);

        const int count = 4;
        var payloads = new byte[count][];

        using (var builder = new RentedMessageBatchBuilder())
        {
            for (var i = 0; i < count; i++)
            {
                payloads[i] = Encoding.UTF8.GetBytes($"no-id-{i}");
                builder.Add(payloads[i]);
            }

            using var batch = builder.Build();
            await client.SendMessagesAsync(Identifier.String(streamName),
                Identifier.String(TopicName), Partitioning.None(), batch.Messages);
        }

        IReadOnlyList<MessageResponse> messages = await PollAll(client, streamName, count);

        // Single partition preserves append order, so payloads come back in the order they were added.
        messages.Count.ShouldBe(count);
        for (var i = 0; i < count; i++)
        {
            messages[i].Payload.ShouldBe(payloads[i]);
        }
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task SendRentedBatch_ToMissingTopic_Should_Throw(Protocol protocol)
    {
        var (client, streamName) = await CreateStreamAndTopic(protocol);

        using var builder = new RentedMessageBatchBuilder();
        builder.Add(Encoding.UTF8.GetBytes("orphan"), Guid.NewGuid());
        using var batch = builder.Build();

        await Should.ThrowAsync<IggyInvalidStatusCodeException>(() =>
            client.SendMessagesAsync(Identifier.String(streamName),
                Identifier.Numeric(999), Partitioning.None(), batch.Messages));
    }
}
