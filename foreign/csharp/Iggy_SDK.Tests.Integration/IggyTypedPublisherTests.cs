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
using System.Text;
using System.Text.Json;
using Apache.Iggy.Contracts;
using Apache.Iggy.Encryption;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Extensions;
using Apache.Iggy.Headers;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Kinds;
using Apache.Iggy.Publishers;
using Apache.Iggy.Shared;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Shouldly;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.Tests.Integrations;

public class IggyTypedPublisherTests
{
    [ClassDataSource<IggyServerFixture>(Shared = SharedType.PerAssembly)]
    public required IggyServerFixture Fixture { get; init; }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task SendAsync_Single_Direct_Should_RoundTrip(Protocol protocol)
    {
        var client = await Client(protocol);
        var stream = await CreateTestStream(client, protocol);

        IggyPublisher<string> publisher = BuildTypedPublisher(client, stream, new Utf8StringSerializer());
        await publisher.InitAsync();

        var id = Guid.NewGuid();
        await publisher.SendAsync("typed-single", id);

        var message = (await Poll(client, stream, 1)).ShouldHaveSingleItem();
        message.Header.Id.ShouldBe(id.ToUInt128());
        Encoding.UTF8.GetString(message.Payload).ShouldBe("typed-single");

        await publisher.DisposeAsync();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task SendAsync_Single_Direct_Should_GenerateId_WhenIdNull(Protocol protocol)
    {
        var client = await Client(protocol);
        var stream = await CreateTestStream(client, protocol);

        IggyPublisher<string> publisher = BuildTypedPublisher(client, stream, new Utf8StringSerializer());
        await publisher.InitAsync();

        await publisher.SendAsync("no-id");

        var message = (await Poll(client, stream, 1)).ShouldHaveSingleItem();
        message.Header.Id.ShouldNotBe((UInt128)0);
        Encoding.UTF8.GetString(message.Payload).ShouldBe("no-id");

        await publisher.DisposeAsync();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task SendAsync_Single_Direct_WithUserHeaders_Should_RoundTrip(Protocol protocol)
    {
        var client = await Client(protocol);
        var stream = await CreateTestStream(client, protocol);

        IggyPublisher<string> publisher = BuildTypedPublisher(client, stream, new Utf8StringSerializer());
        await publisher.InitAsync();

        var headers = new Dictionary<HeaderKey, HeaderValue>
        {
            [HeaderKey.FromString("source")] = HeaderValue.FromString("typed-test")
        };
        await publisher.SendAsync("with-headers", userHeaders: headers);

        var message = (await Poll(client, stream, 1)).ShouldHaveSingleItem();
        Encoding.UTF8.GetString(message.Payload).ShouldBe("with-headers");
        message.UserHeaders.ShouldNotBeNull();
        message.UserHeaders!.ShouldContainKey(HeaderKey.FromString("source"));
        Encoding.UTF8.GetString(message.UserHeaders[HeaderKey.FromString("source")].Value).ShouldBe("typed-test");

        await publisher.DisposeAsync();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task SendAsync_Batch_Direct_Should_RoundTripAll(Protocol protocol)
    {
        var client = await Client(protocol);
        var stream = await CreateTestStream(client, protocol);

        IggyPublisher<string> publisher = BuildTypedPublisher(client, stream, new Utf8StringSerializer());
        await publisher.InitAsync();

        List<string> payloads = Enumerable.Range(0, 10).Select(i => $"batch-{i}").ToList();
        await publisher.SendAsync(payloads);

        List<MessageResponse> messages = await Poll(client, stream, payloads.Count);
        messages.Select(m => Encoding.UTF8.GetString(m.Payload)).ShouldBe(payloads, true);

        await publisher.DisposeAsync();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task SendAsync_Items_Direct_Should_RoundTrip_WithIdsAndHeaders(Protocol protocol)
    {
        var client = await Client(protocol);
        var stream = await CreateTestStream(client, protocol);

        IggyPublisher<string> publisher = BuildTypedPublisher(client, stream, new Utf8StringSerializer());
        await publisher.InitAsync();

        var firstId = Guid.NewGuid();
        var headers = new Dictionary<HeaderKey, HeaderValue>
        {
            [HeaderKey.FromString("k")] = HeaderValue.FromString("v")
        };
        var items = new (string, Guid?, Dictionary<HeaderKey, HeaderValue>?)[]
        {
            ("item-a", firstId, headers), ("item-b", null, null)
        };

        await publisher.SendAsync(items);

        List<MessageResponse> messages = await Poll(client, stream, 2);
        Dictionary<UInt128, MessageResponse> byId = messages.ToDictionary(m => m.Header.Id);

        byId.ShouldContainKey(firstId.ToUInt128());
        var first = byId[firstId.ToUInt128()];
        Encoding.UTF8.GetString(first.Payload).ShouldBe("item-a");
        first.UserHeaders.ShouldNotBeNull();
        first.UserHeaders!.ShouldContainKey(HeaderKey.FromString("k"));

        messages.Select(m => Encoding.UTF8.GetString(m.Payload)).ShouldContain("item-b");

        await publisher.DisposeAsync();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task SendAsync_Single_Background_Should_RoundTrip(Protocol protocol)
    {
        var client = await Client(protocol);
        var stream = await CreateTestStream(client, protocol);

        IggyPublisher<string> publisher = BuildTypedPublisher(client, stream, new Utf8StringSerializer(), true);
        await publisher.InitAsync();

        var id = Guid.NewGuid();
        await publisher.SendAsync("bg-single", id);
        await publisher.WaitUntilAllSendsAsync();

        var message = (await Poll(client, stream, 1)).ShouldHaveSingleItem();
        message.Header.Id.ShouldBe(id.ToUInt128());
        Encoding.UTF8.GetString(message.Payload).ShouldBe("bg-single");

        await publisher.DisposeAsync();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task SendAsync_Batch_Background_Should_RoundTripAll(Protocol protocol)
    {
        var client = await Client(protocol);
        var stream = await CreateTestStream(client, protocol);

        IggyPublisher<string> publisher = BuildTypedPublisher(client, stream, new Utf8StringSerializer(), true);
        await publisher.InitAsync();

        List<string> payloads = Enumerable.Range(0, 25).Select(i => $"bg-batch-{i}").ToList();
        await publisher.SendAsync(payloads);
        await publisher.WaitUntilAllSendsAsync();

        List<MessageResponse> messages = await Poll(client, stream, payloads.Count);
        messages.Select(m => Encoding.UTF8.GetString(m.Payload)).ShouldBe(payloads, true);

        await publisher.DisposeAsync();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task SendAsync_Items_Background_Should_RoundTrip_WithIdsAndHeaders(Protocol protocol)
    {
        var client = await Client(protocol);
        var stream = await CreateTestStream(client, protocol);

        IggyPublisher<string> publisher = BuildTypedPublisher(client, stream, new Utf8StringSerializer(), true);
        await publisher.InitAsync();

        var id = Guid.NewGuid();
        var headers = new Dictionary<HeaderKey, HeaderValue>
        {
            [HeaderKey.FromString("k")] = HeaderValue.FromString("v")
        };
        var items = new (string, Guid?, Dictionary<HeaderKey, HeaderValue>?)[] { ("bg-item", id, headers) };

        await publisher.SendAsync(items);
        await publisher.WaitUntilAllSendsAsync();

        var message = (await Poll(client, stream, 1)).ShouldHaveSingleItem();
        message.Header.Id.ShouldBe(id.ToUInt128());
        Encoding.UTF8.GetString(message.Payload).ShouldBe("bg-item");
        message.UserHeaders.ShouldNotBeNull();
        message.UserHeaders!.ShouldContainKey(HeaderKey.FromString("k"));

        await publisher.DisposeAsync();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task SendAsync_WithoutInit_Should_Throw_PublisherNotInitializedException(Protocol protocol)
    {
        var client = await Client(protocol);
        var stream = await CreateTestStream(client, protocol);

        IggyPublisher<string> publisher = BuildTypedPublisher(client, stream, new Utf8StringSerializer());

        await Should.ThrowAsync<PublisherNotInitializedException>(() => publisher.SendAsync("nope"));
        await publisher.DisposeAsync();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task SendAsync_WithEncryptor_Should_RoundTrip_Decrypted(Protocol protocol)
    {
        var encryptor = new AesMessageEncryptor(AesMessageEncryptor.GenerateKey());

        // Encryption is configured on the client. The publisher uses an encrypting client; a plain client polls
        // to prove the wire bytes are ciphertext, then decrypts manually.
        var encryptingClient = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient(encryptor: encryptor)
            : await Fixture.CreateHttpClient(encryptor: encryptor);
        var plainClient = await Client(protocol);
        var stream = await CreateTestStream(plainClient, protocol);

        IggyPublisher<string> publisher = BuildTypedPublisher(encryptingClient, stream, new Utf8StringSerializer());
        await publisher.InitAsync();

        await publisher.SendAsync("secret-payload");

        var message = (await Poll(plainClient, stream, 1)).ShouldHaveSingleItem();
        Encoding.UTF8.GetString(message.Payload).ShouldNotBe("secret-payload");
        Encoding.UTF8.GetString(encryptor.DecryptToArray(message.Payload)).ShouldBe("secret-payload");

        await publisher.DisposeAsync();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task SendAsync_WithJsonSerializer_Should_RoundTrip(Protocol protocol)
    {
        var client = await Client(protocol);
        var stream = await CreateTestStream(client, protocol);

        IggyPublisher<SampleEvent> publisher
            = BuildTypedPublisher(client, stream, new SystemTextJsonSerializer<SampleEvent>());
        await publisher.InitAsync();

        var sent = new[] { new SampleEvent(1, "alice"), new SampleEvent(2, "bob") };
        await publisher.SendAsync(sent);

        List<MessageResponse> messages = await Poll(client, stream, sent.Length);
        SampleEvent[] decoded = messages
            .Select(m => JsonSerializer.Deserialize<SampleEvent>(m.Payload)!)
            .ToArray();
        decoded.ShouldBe(sent, true);

        await publisher.DisposeAsync();
    }

    private async Task<IIggyClient> Client(Protocol protocol)
    {
        return protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();
    }

    // Base fluent methods return the non-generic builder, so apply them as statements to keep the typed Build().
    private static IggyPublisher<T> BuildTypedPublisher<T>(IIggyClient client, TestStreamInfo stream,
        ISerializer<T> serializer, bool background = false)
    {
        IggyPublisherBuilder<T> builder = IggyPublisherBuilder<T>.Create(client, Identifier.String(stream.StreamId),
            Identifier.String(stream.TopicId), serializer);
        builder.WithPartitioning(Partitioning.PartitionId(0));
        if (background)
        {
            builder.WithBackgroundSending(true, 1000, 10, TimeSpan.FromMilliseconds(50));
        }

        return builder.Build();
    }

    private static async Task<List<MessageResponse>> Poll(IIggyClient client, TestStreamInfo stream, int count)
    {
        var polled = await client.PollMessagesAsync(Identifier.String(stream.StreamId),
            Identifier.String(stream.TopicId),
            0,
            Consumer.New(1),
            PollingStrategy.First(),
            (uint)count,
            false);

        return polled.Messages.ToList();
    }

    private async Task<TestStreamInfo> CreateTestStream(IIggyClient client, Protocol protocol)
    {
        var streamId = $"typed_pub_{Guid.NewGuid()}_{protocol.ToString().ToLowerInvariant()}";
        var topicId = "test_topic";

        await client.CreateStreamAsync(streamId);
        await client.CreateTopicAsync(Identifier.String(streamId), topicId, 1);

        return new TestStreamInfo(streamId, topicId);
    }

    private record TestStreamInfo(string StreamId, string TopicId);

    private record SampleEvent(int Id, string Name);

    private sealed class Utf8StringSerializer : ISerializer<string>
    {
        public void Serialize(string data, IBufferWriter<byte> writer)
        {
            writer.Write(Encoding.UTF8.GetBytes(data));
        }
    }
}
