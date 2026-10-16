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
using Apache.Iggy.Contracts;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Extensions;
using Apache.Iggy.Headers;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Messages;
using Apache.Iggy.Publishers;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.Tests.PublisherTests;

public class IggyTypedPublisherTests
{
    private static readonly Identifier Stream = Identifier.Numeric(1);
    private static readonly Identifier Topic = Identifier.Numeric(2);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task SendSingle_Direct_Should_SendOneMessage_WithProvidedId_AndSerializedPayload()
    {
        var recorder = new SendRecorder();
        await using IggyPublisher<string> publisher = await CreatePublisherAsync(BuildClient(recorder), Config());
        var id = Guid.NewGuid();

        await publisher.SendAsync("hello", id, ct: Ct);

        var sent = Assert.Single(recorder.AllMessages);
        Assert.Equal(id.ToUInt128(), sent.Id);
        Assert.Equal("hello", Decode(sent));
        Assert.Null(sent.UserHeaders);
    }

    [Fact]
    public async Task SendSingle_Direct_Should_SendZeroId_WhenMessageIdNull()
    {
        var recorder = new SendRecorder();
        await using IggyPublisher<string> publisher = await CreatePublisherAsync(BuildClient(recorder), Config());

        await publisher.SendAsync("hello", ct: Ct);

        var sent = Assert.Single(recorder.AllMessages);
        Assert.Equal(UInt128.Zero, sent.Id);
        Assert.Equal("hello", Decode(sent));
    }

    [Fact]
    public async Task SendSingle_Direct_Should_AttachUserHeaders()
    {
        var recorder = new SendRecorder();
        await using IggyPublisher<string> publisher = await CreatePublisherAsync(BuildClient(recorder), Config());
        var headers = new Dictionary<HeaderKey, HeaderValue>
        {
            [HeaderKey.FromString("k")] = HeaderValue.FromString("v")
        };

        await publisher.SendAsync("hello", userHeaders: headers, ct: Ct);

        var sent = Assert.Single(recorder.AllMessages);
        Assert.NotNull(sent.UserHeaders);
        Assert.Contains(HeaderKey.FromString("k"), sent.UserHeaders);
    }

    [Fact]
    public async Task SendBatch_Direct_Should_SerializeEachItem_InOrder()
    {
        var recorder = new SendRecorder();
        await using IggyPublisher<string> publisher = await CreatePublisherAsync(BuildClient(recorder), Config());

        await publisher.SendAsync(new[] { "a", "b", "c" }, Ct);

        Assert.Equal(["a", "b", "c"], recorder.AllMessages.Select(Decode));
    }

    [Fact]
    public async Task SendBatch_Direct_Should_NotCallClient_WhenEmpty()
    {
        var recorder = new SendRecorder();
        await using IggyPublisher<string> publisher = await CreatePublisherAsync(BuildClient(recorder), Config());

        await publisher.SendAsync(Array.Empty<string>(), Ct);

        Assert.Empty(recorder.Calls);
    }

    [Fact]
    public async Task SendItems_Direct_Should_RespectProvidedIdsAndHeaders()
    {
        var recorder = new SendRecorder();
        await using IggyPublisher<string> publisher = await CreatePublisherAsync(BuildClient(recorder), Config());
        var id = Guid.NewGuid();
        var headers = new Dictionary<HeaderKey, HeaderValue>
        {
            [HeaderKey.FromString("k")] = HeaderValue.FromString("v")
        };

        await publisher.SendAsync(
            new (string, Guid?, Dictionary<HeaderKey, HeaderValue>?)[] { ("a", id, headers), ("b", null, null) }, Ct);

        Assert.Equal(2, recorder.AllMessages.Count);
        var first = recorder.AllMessages[0];
        Assert.Equal(id.ToUInt128(), first.Id);
        Assert.Equal("a", Decode(first));
        Assert.NotNull(first.UserHeaders);
        Assert.Contains(HeaderKey.FromString("k"), first.UserHeaders);

        var second = recorder.AllMessages[1];
        Assert.Equal("b", Decode(second));
        Assert.Equal(UInt128.Zero, second.Id);
        Assert.Null(second.UserHeaders);
    }

    [Fact]
    public async Task Send_Direct_Should_ForwardStreamTopicAndPartitioning()
    {
        var recorder = new SendRecorder();
        IggyPublisherConfig<string> config = Config();
        config.Partitioning = Partitioning.PartitionId(7);
        await using IggyPublisher<string> publisher = await CreatePublisherAsync(BuildClient(recorder), config);

        await publisher.SendAsync("hello", ct: Ct);

        var (stream, topic, partitioning, _) = Assert.Single(recorder.Calls);
        Assert.Equal(Stream, stream);
        Assert.Equal(Topic, topic);
        Assert.Equal(Enums.Partitioning.PartitionId, partitioning.Kind);
    }

    [Fact]
    public async Task SendSingle_Should_Throw_WhenNotInitialized()
    {
        var publisher = new IggyPublisher<string>(BuildClient(new SendRecorder()), Config(),
            NullLogger<IggyPublisher<string>>.Instance);

        await Assert.ThrowsAsync<PublisherNotInitializedException>(() => publisher.SendAsync("hello", ct: Ct));
        await publisher.DisposeAsync();
    }

    [Fact]
    public async Task SendBatch_Should_Throw_WhenNotInitialized()
    {
        var publisher = new IggyPublisher<string>(BuildClient(new SendRecorder()), Config(),
            NullLogger<IggyPublisher<string>>.Instance);

        await Assert.ThrowsAsync<PublisherNotInitializedException>(() => publisher.SendAsync(new[] { "a" }, Ct));
        await publisher.DisposeAsync();
    }

    [Fact]
    public async Task SendItems_Should_Throw_WhenNotInitialized()
    {
        var publisher = new IggyPublisher<string>(BuildClient(new SendRecorder()), Config(),
            NullLogger<IggyPublisher<string>>.Instance);

        await Assert.ThrowsAsync<PublisherNotInitializedException>(() => publisher.SendAsync(
            new (string, Guid?, Dictionary<HeaderKey, HeaderValue>?)[] { ("a", null, null) }, Ct));
        await publisher.DisposeAsync();
    }

    [Fact]
    public async Task SendSingle_Background_Should_SendAfterDrain_WithProvidedId_AndSerializedPayload()
    {
        var recorder = new SendRecorder();
        await using IggyPublisher<string> publisher = await CreatePublisherAsync(BuildClient(recorder), Config(true));
        var id = Guid.NewGuid();

        await publisher.SendAsync("hello", id, ct: Ct);
        await publisher.WaitUntilAllSendsAsync(Ct);

        var sent = Assert.Single(recorder.AllMessages);
        Assert.Equal(id.ToUInt128(), sent.Id);
        Assert.Equal("hello", Decode(sent));
    }

    [Fact]
    public async Task SendBatch_Background_Should_SendAllAfterDrain()
    {
        var recorder = new SendRecorder();
        await using IggyPublisher<string> publisher = await CreatePublisherAsync(BuildClient(recorder), Config(true));

        await publisher.SendAsync(new[] { "a", "b", "c" }, Ct);
        await publisher.WaitUntilAllSendsAsync(Ct);

        Assert.Equivalent(new[] { "a", "b", "c" }, recorder.AllMessages.Select(Decode), true);
    }

    [Fact]
    public async Task SendItems_Background_Should_RespectProvidedIdsAndHeaders()
    {
        var recorder = new SendRecorder();
        await using IggyPublisher<string> publisher = await CreatePublisherAsync(BuildClient(recorder), Config(true));
        var id = Guid.NewGuid();
        var headers = new Dictionary<HeaderKey, HeaderValue>
        {
            [HeaderKey.FromString("k")] = HeaderValue.FromString("v")
        };

        await publisher.SendAsync(new (string, Guid?, Dictionary<HeaderKey, HeaderValue>?)[] { ("a", id, headers) },
            Ct);
        await publisher.WaitUntilAllSendsAsync(Ct);

        var sent = Assert.Single(recorder.AllMessages);
        Assert.Equal(id.ToUInt128(), sent.Id);
        Assert.Equal("a", Decode(sent));
        Assert.NotNull(sent.UserHeaders);
        Assert.Contains(HeaderKey.FromString("k"), sent.UserHeaders);
    }

    [Fact]
    public async Task SendBatch_Background_Should_NotSend_WhenEmpty()
    {
        var recorder = new SendRecorder();
        await using IggyPublisher<string> publisher = await CreatePublisherAsync(BuildClient(recorder), Config(true));

        await publisher.SendAsync(Array.Empty<string>(), Ct);
        await publisher.WaitUntilAllSendsAsync(Ct);

        Assert.Empty(recorder.Calls);
    }

    [Fact]
    public async Task SendBackground_Should_DrainEveryMessage_WhenManyProducersEnqueueConcurrently()
    {
        const int producers = 16;
        const int perProducer = 50;

        var recorder = new SendRecorder();
        await using IggyPublisher<string> publisher = await CreatePublisherAsync(BuildClient(recorder), Config(true));

        // Hammer the in-flight accounting from many threads; a missed 1->0 signal would hang the drain.
        IEnumerable<Task> sends = Enumerable.Range(0, producers).Select(producer => Task.Run(async () =>
        {
            for (var i = 0; i < perProducer; i++)
            {
                await publisher.SendAsync($"p{producer}-{i}", ct: Ct);
            }
        }, Ct));

        await Task.WhenAll(sends);
        await publisher.WaitUntilAllSendsAsync(Ct);

        IEnumerable<string> expected = Enumerable.Range(0, producers)
            .SelectMany(producer => Enumerable.Range(0, perProducer).Select(i => $"p{producer}-{i}"));
        Assert.Equivalent(expected, recorder.AllMessages.Select(Decode), true);
    }

    [Theory]
    [InlineData(true, true)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(false, false)]
    public async Task DisposeAsync_AfterLogoutFailure_Should_DisposeOnlyOwnedClient(bool ownsClient, bool initialized)
    {
        var client = Mock.Get(BuildClient(new SendRecorder()));
        client.Setup(value => value.LogoutUserAsync(It.IsAny<CancellationToken>()))
            .ThrowsAsync(new IOException("Connection lost during logout."));
        var config = Config();
        config.CreateIggyClient = ownsClient;
        var publisher = new IggyPublisher<string>(client.Object, config,
            NullLogger<IggyPublisher<string>>.Instance);

        if (initialized)
        {
            await publisher.InitAsync(Ct);
        }
        else
        {
            client.Setup(value => value.GetStreamByIdAsync(It.IsAny<Identifier>(), It.IsAny<CancellationToken>()))
                .ThrowsAsync(new IOException("Connection lost during initialization."));
            await Assert.ThrowsAsync<IOException>(() => publisher.InitAsync(Ct));
        }

        await publisher.DisposeAsync();
        await publisher.DisposeAsync();

        client.Verify(value => value.Dispose(), ownsClient ? Times.Once() : Times.Never());
        client.Verify(value => value.LogoutUserAsync(It.IsAny<CancellationToken>()),
            ownsClient && initialized ? Times.Once() : Times.Never());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DisposeAsync_ConcurrentCalls_Should_DisposeOwnedClientOnce(bool logoutFails)
    {
        var logout = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var client = Mock.Get(BuildClient(new SendRecorder()));
        client.Setup(value => value.LogoutUserAsync(It.IsAny<CancellationToken>()))
            .Returns(logout.Task);
        var config = Config();
        config.CreateIggyClient = true;
        var publisher = await CreatePublisherAsync(client.Object, config);

        var first = publisher.DisposeAsync().AsTask();
        var second = publisher.DisposeAsync().AsTask();
        if (logoutFails)
        {
            logout.SetException(new IOException("Connection lost during logout."));
        }
        else
        {
            logout.SetResult();
        }

        await Task.WhenAll(first, second).WaitAsync(Ct);

        client.Verify(value => value.LogoutUserAsync(It.IsAny<CancellationToken>()), Times.Once());
        client.Verify(value => value.Dispose(), Times.Once());
    }

    [Fact]
    public async Task DisposeAsync_WhenBackgroundCleanupFails_Should_DisposeOwnedClient()
    {
        var client = Mock.Get(BuildClient(new SendRecorder()));
        client.Setup(value => value.UnsubscribeConnectionEvents(
                It.IsAny<Func<ConnectionStateChangedEventArgs, Task>>()))
            .Throws(new IOException("Connection cleanup failed."));
        var config = Config(true);
        config.CreateIggyClient = true;
        var publisher = new IggyPublisher<string>(client.Object, config,
            NullLogger<IggyPublisher<string>>.Instance);

        await Assert.ThrowsAsync<IOException>(() => publisher.DisposeAsync().AsTask());
        await publisher.DisposeAsync();

        client.Verify(value => value.Dispose(), Times.Once());
        client.Verify(value => value.LogoutUserAsync(It.IsAny<CancellationToken>()), Times.Never());
    }

    private static string Decode(SentMessage message)
    {
        return Encoding.UTF8.GetString(message.Payload);
    }

    private static IggyPublisherConfig<string> Config(bool background = false)
    {
        return new IggyPublisherConfig<string>
        {
            StreamId = Stream,
            TopicId = Topic,
            Partitioning = Partitioning.None(),
            Serializer = new StringSerializer(),
            EnableBackgroundSending = background,
            BackgroundFlushInterval = TimeSpan.FromMilliseconds(15),
            BackgroundBatchSize = 100,
            BackgroundQueueCapacity = 1000
        };
    }

    private static async Task<IggyPublisher<string>> CreatePublisherAsync(IIggyClient client,
        IggyPublisherConfig<string> config)
    {
        var publisher = new IggyPublisher<string>(client, config, NullLogger<IggyPublisher<string>>.Instance);
        await publisher.InitAsync(Ct);
        return publisher;
    }

    // Captures every send, copying payloads out of the pooled buffers before they are recycled.
    private static IIggyClient BuildClient(SendRecorder recorder)
    {
        var mock = new Mock<IIggyClient>(MockBehavior.Loose);
        mock.Setup(c => c.ConnectAsync(It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
        mock.Setup(c => c.GetStreamByIdAsync(It.IsAny<Identifier>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(StreamResponse());
        mock.Setup(c => c.GetTopicByIdAsync(It.IsAny<Identifier>(), It.IsAny<Identifier>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(TopicResponse());

        mock.Setup(c => c.SendMessagesAsync(It.IsAny<Identifier>(), It.IsAny<Identifier>(),
                It.IsAny<Partitioning>(), It.IsAny<IList<Message>>(), It.IsAny<CancellationToken>()))
            .Returns((Identifier stream, Identifier topic, Partitioning partitioning, IList<Message> messages,
                CancellationToken _) =>
            {
                recorder.Record(stream, topic, partitioning, messages);
                return Task.FromResult(new SendMessagesResponse { Confirmations = [] });
            });
        mock.Setup(c => c.SendMessagesAsync(It.IsAny<Identifier>(), It.IsAny<Identifier>(),
                It.IsAny<Partitioning>(), It.IsAny<Message>(), It.IsAny<CancellationToken>()))
            .Returns((Identifier stream, Identifier topic, Partitioning partitioning, Message message,
                CancellationToken _) =>
            {
                recorder.Record(stream, topic, partitioning, new[] { message });
                return Task.FromResult(new SendMessagesResponse { Confirmations = [] });
            });

        return mock.Object;
    }

    private static StreamResponse StreamResponse()
    {
        return new StreamResponse
        {
            Id = 1,
            Name = "stream",
            Size = 0,
            CreatedAt = default,
            MessagesCount = 0,
            TopicsCount = 0
        };
    }

    private static TopicResponse TopicResponse()
    {
        return new TopicResponse
        {
            Id = 2,
            Name = "topic",
            Size = 0,
            CreatedAt = default,
            MaxTopicSize = 0,
            MessagesCount = 0,
            PartitionsCount = 1
        };
    }

    private sealed class SendRecorder
    {
        public List<(Identifier Stream, Identifier Topic, Partitioning Partitioning, List<SentMessage> Messages)> Calls
        {
            get;
        } = new();

        public IReadOnlyList<SentMessage> AllMessages => Calls.SelectMany(c => c.Messages).ToList();

        public void Record(Identifier stream, Identifier topic, Partitioning partitioning, IList<Message> messages)
        {
            List<SentMessage> captured = messages.Select(m => new SentMessage
            {
                Id = m.Header.Id,
                Payload = m.Payload.ToArray(),
                UserHeaders = m.UserHeaders
            }).ToList();
            Calls.Add((stream, topic, partitioning, captured));
        }
    }

    private sealed class SentMessage
    {
        public UInt128 Id { get; init; }
        public byte[] Payload { get; init; } = Array.Empty<byte>();
        public Dictionary<HeaderKey, HeaderValue>? UserHeaders { get; init; }
    }

    private sealed class StringSerializer : ISerializer<string>
    {
        public void Serialize(string data, IBufferWriter<byte> writer)
        {
            writer.Write(Encoding.UTF8.GetBytes(data));
        }
    }
}
