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
using System.Text;
using Apache.Iggy.Consumers;
using Apache.Iggy.Contracts;
using Apache.Iggy.Encryption;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Extensions;
using Apache.Iggy.Headers;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Kinds;
using Apache.Iggy.Messages;
using Apache.Iggy.Publishers;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Apache.Iggy.Vsr;
using Shouldly;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.Tests.Integrations;

public class MessageEncryptionIntegrationTests
{
    private const int BatchSize = 25;

    [ClassDataSource<IggyServerFixture>(Shared = SharedType.PerAssembly)]
    public required IggyServerFixture Fixture { get; init; }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task PollRented_WithEncryptor_Should_DecryptBatchOfPayloads(Protocol protocol)
    {
        var encryptor = CreateEncryptor();
        var client = await CreateClient(protocol, encryptor);
        var (streamId, topicId) = await CreateTestStream(client, protocol);

        await SendBatch(client, streamId, topicId, false);

        using var rental = await client.PollMessagesRentedAsync(streamId, topicId, 0, Consumer.New(1),
            PollingStrategy.Offset(0), BatchSize, false);

        rental.Messages.Count.ShouldBe(BatchSize);
        for (var i = 0; i < BatchSize; i++)
        {
            var message = rental.Messages[i];
            Encoding.UTF8.GetString(message.Payload.Span).ShouldBe($"payload-{i}");
            message.RawUserHeaders.IsEmpty.ShouldBeTrue();
            message.Header.UserHeadersLength.ShouldBe(0);
            message.Header.PayloadLength.ShouldBe(Encoding.UTF8.GetByteCount($"payload-{i}"));
        }
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task PollRented_WithEncryptor_Should_DecryptBatchWithHeaders(Protocol protocol)
    {
        var encryptor = CreateEncryptor();
        var client = await CreateClient(protocol, encryptor);
        var (streamId, topicId) = await CreateTestStream(client, protocol);

        // Payload and headers both encrypted: both blobs per message must decrypt into the single rented buffer
        // at the right offsets.
        await SendBatch(client, streamId, topicId, true);

        using var rental = await client.PollMessagesRentedAsync(streamId, topicId, 0, Consumer.New(1),
            PollingStrategy.Offset(0), BatchSize, false);

        rental.Messages.Count.ShouldBe(BatchSize);
        for (var i = 0; i < BatchSize; i++)
        {
            var message = rental.Messages[i];
            Encoding.UTF8.GetString(message.Payload.Span).ShouldBe($"payload-{i}");

            message.RawUserHeaders.IsEmpty.ShouldBeFalse();
            Dictionary<HeaderKey, HeaderValue>? headers = message.UserHeaders;
            headers.ShouldNotBeNull();
            var typeHeader = headers![new HeaderKey
            {
                Kind = HeaderKind.String,
                Value = "type"u8.ToArray()
            }];
            Encoding.UTF8.GetString(typeHeader.Value).ShouldBe("test-message");
        }
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task PollMessages_WithEncryptor_Should_DecryptBatch(Protocol protocol)
    {
        var encryptor = CreateEncryptor();
        var client = await CreateClient(protocol, encryptor);
        var (streamId, topicId) = await CreateTestStream(client, protocol);

        // Materialized poll path (allocates owned arrays): same batch, decrypted before the rental is disposed.
        await SendBatch(client, streamId, topicId, true);

        var polled = await client.PollMessagesAsync(streamId, topicId, 0, Consumer.New(1), PollingStrategy.Offset(0),
            BatchSize, false);

        polled.Messages.Count.ShouldBe(BatchSize);
        for (var i = 0; i < BatchSize; i++)
        {
            Encoding.UTF8.GetString(polled.Messages[i].Payload).ShouldBe($"payload-{i}");
            polled.Messages[i].UserHeaders.ShouldNotBeNull();
        }
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task PollMessages_WithWrongKey_Should_ThrowMessageDecryptionException(Protocol protocol)
    {
        var publisherEncryptor = CreateEncryptor();
        var wrongKeyEncryptor = CreateEncryptor();

        var publisherClient = await CreateClient(protocol, publisherEncryptor);
        var (streamId, topicId) = await CreateTestStream(publisherClient, protocol);
        await SendBatch(publisherClient, streamId, topicId, false);

        // Same wire bytes, a different key: decryption must fail loudly rather than hand back garbage.
        var wrongKeyClient = await CreateClient(protocol, wrongKeyEncryptor);

        await Should.ThrowAsync<MessageDecryptionException>(async () =>
            await wrongKeyClient.PollMessagesAsync(streamId, topicId, 0, Consumer.New(2), PollingStrategy.Offset(0),
                BatchSize, false));
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task ReceiveAsync_WithWrongKey_Should_ThrowMessageDecryptionException(Protocol protocol)
    {
        var publisherEncryptor = CreateEncryptor();
        var wrongKeyEncryptor = CreateEncryptor();

        var publisherClient = await CreateClient(protocol, publisherEncryptor);
        var (streamId, topicId) = await CreateTestStream(publisherClient, protocol);
        await SendBatch(publisherClient, streamId, topicId, false);

        var wrongKeyClient = await CreateClient(protocol, wrongKeyEncryptor);
        var consumer = IggyConsumerBuilder
            .Create(wrongKeyClient, streamId, topicId, Consumer.New(3))
            .WithPollingStrategy(PollingStrategy.First())
            .WithPartitions(0)
            .Build();

        await consumer.InitAsync();

        // A bad key is not transient: it must surface out of the stream with the continuation that skips it.
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var error = await Should.ThrowAsync<ReadException>(async () =>
        {
            await foreach (var _ in consumer.ReceiveAsync(cts.Token))
            {
                break;
            }
        });
        error.InnerException.ShouldBeOfType<MessageDecryptionException>();
        error.Skip.ShouldNotBeNull();

        await consumer.DisposeAsync();
    }

    [Test]
    public async Task Reader_WithWrongKey_Should_ReportTheRecordAsServed()
    {
        var publisherClient = await CreateClient(Protocol.Tcp, CreateEncryptor());
        var (streamId, topicId) = await CreateTestStream(publisherClient, Protocol.Tcp);
        await SendBatch(publisherClient, streamId, topicId, true);

        var wrongKeyClient = await CreateClient(Protocol.Tcp, CreateEncryptor());
        await using var reader = await wrongKeyClient.Reader(streamId, topicId, Consumer.New(4))
            .WithPartitions(0)
            .WithStart(PollingStrategy.First())
            .BuildAsync();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var error = await Should.ThrowAsync<ReadException>(async () => await reader.NextAsync(cts.Token));
        await reader.SeekAsync(0, error.Skip!.Value, cts.Token);
        var next = await Should.ThrowAsync<ReadException>(async () => await reader.NextAsync(cts.Token));

        error.InnerException.ShouldBeOfType<MessageDecryptionException>();
        error.Offset.ShouldBe(0ul);
        error.Record.ShouldBe(await FrameAsServed(wrongKeyClient, streamId, topicId, 0));
        next.Offset.ShouldBe(1ul);
        next.Record.ShouldBe(await FrameAsServed(wrongKeyClient, streamId, topicId, 1));
    }

    /// <summary>The ciphertext frame of the message at <paramref name="offset" />, as a fetch serves it.</summary>
    private static async Task<byte[]> FrameAsServed(IIggyClient client, Identifier streamId, Identifier topicId,
        ulong offset)
    {
        using var fetched = await ((IPartitionFetchClient)client).FetchMessagesAsync(streamId, topicId,
            [(0u, PollingStrategy.Offset(offset))], Consumer.New(5), 1, false, new PollOptions().Wire);
        var records = fetched.Sections.Single().Records.Span;
        var frame = records[BatchWireFormat.BATCH_HEADER_SIZE..];
        var headersLength = BinaryPrimitives.ReadInt32LittleEndian(frame[32..]);
        var payloadLength = BinaryPrimitives.ReadInt32LittleEndian(frame[36..]);
        return frame[..(BatchWireFormat.FRAME_HEADER_SIZE + payloadLength + headersLength)].ToArray();
    }

    private async Task SendBatch(IIggyClient client, Identifier streamId, Identifier topicId, bool withHeaders)
    {
        var publisher = IggyPublisherBuilder
            .Create(client, streamId, topicId)
            .WithPartitioning(Partitioning.PartitionId(0))
            .Build();

        await publisher.InitAsync();

        var messages = new List<Message>(BatchSize);
        for (var i = 0; i < BatchSize; i++)
        {
            var payload = Encoding.UTF8.GetBytes($"payload-{i}");
            messages.Add(withHeaders
                ? new Message(Guid.NewGuid(), payload, CreateTestHeaders())
                : new Message(Guid.NewGuid(), payload));
        }

        await publisher.SendMessagesAsync(messages);
        await publisher.DisposeAsync();
    }

    private Task<IIggyClient> CreateClient(Protocol protocol, IMessageEncryptor encryptor)
    {
        return Fixture.CreateAuthenticatedClient(protocol, encryptor: encryptor);
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
                new HeaderKey
                {
                    Kind = HeaderKind.String,
                    Value = "type"u8.ToArray()
                },
                new HeaderValue
                {
                    Kind = HeaderKind.String,
                    Value = "test-message"u8.ToArray()
                }
            }
        };
    }

    private static async Task<(Identifier StreamId, Identifier TopicId)> CreateTestStream(IIggyClient client,
        Protocol protocol)
    {
        var streamId = $"enc_msg_stream_{Guid.NewGuid()}_{protocol.ToString().ToLowerInvariant()}";
        var topicId = "enc_msg_topic";

        await client.CreateStreamAsync(streamId);
        await client.CreateTopicAsync(Identifier.String(streamId), topicId, 1);

        return (Identifier.String(streamId), Identifier.String(topicId));
    }
}
