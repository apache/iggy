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
using Apache.Iggy.Consumers;
using Apache.Iggy.Contracts;
using Apache.Iggy.Encryption;
using Apache.Iggy.Exceptions;
using Apache.Iggy.IggyClient;
using Apache.Iggy.IggyClient.Implementations;
using Apache.Iggy.Kinds;
using Apache.Iggy.Messages;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace Apache.Iggy.Tests.ConsumerTests;

public class RentedConsumerTests
{
    [Fact]
    public async Task ReceiveRentedAsync_Should_YieldMessages_WithExpectedPayloads()
    {
        var owner = new TrackingMemoryOwner(1024);
        IReadOnlyList<RentedMessageResponse> messages = BuildMessages(owner, 3);
        var rental = new PolledMessagesRental(owner)
        {
            PartitionId = 1,
            CurrentOffset = 2,
            Messages = messages
        };
        Mock<IIggyClient> client = BuildClientMock(new Queue<PolledMessagesRental>(new[] { rental }));
        var consumer = new IggyConsumer(client.Object, BuildConfig(), NullLoggerFactory.Instance);
        await consumer.InitAsync(TestContext.Current.CancellationToken);

        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var got = new List<ReceivedRentedMessage>();
        await using (IAsyncEnumerator<ReceivedRentedMessage> e = consumer.ReceiveRentedAsync(cts.Token)
                         .GetAsyncEnumerator(TestContext.Current.CancellationToken))
        {
            for (var i = 0; i < 3; i++)
            {
                Assert.True(await e.MoveNextAsync());
                got.Add(e.Current);
            }
        }

        Assert.Equal(3, got.Count);
        for (var i = 0; i < 3; i++)
        {
            Assert.Equal(MessageStatus.Success, got[i].Status);
            Assert.Equal($"msg-{i}", Encoding.UTF8.GetString(got[i].Message.Payload.Span));
            Assert.Equal((ulong)i, got[i].CurrentOffset);
            Assert.Equal(1u, got[i].PartitionId);
            Assert.Null(got[i].Error);
        }

        // Buffer still rented — none of the messages have been disposed.
        Assert.Equal(0, owner.DisposeCount);

        foreach (var m in got)
        {
            m.Dispose();
        }

        // After disposing every message of the batch, rental returned to pool exactly once.
        Assert.Equal(1, owner.DisposeCount);
        await consumer.DisposeAsync();
    }

    [Fact]
    public async Task ReceiveRentedAsync_Should_NotReturnBuffer_UntilAllMessagesDisposed()
    {
        var owner = new TrackingMemoryOwner(1024);
        IReadOnlyList<RentedMessageResponse> messages = BuildMessages(owner, 3);
        var rental = new PolledMessagesRental(owner)
        {
            PartitionId = 1,
            CurrentOffset = 2,
            Messages = messages
        };
        Mock<IIggyClient> client = BuildClientMock(new Queue<PolledMessagesRental>(new[] { rental }));
        var consumer = new IggyConsumer(client.Object, BuildConfig(), NullLoggerFactory.Instance);
        await consumer.InitAsync(TestContext.Current.CancellationToken);

        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var got = new List<ReceivedRentedMessage>();
        await using (IAsyncEnumerator<ReceivedRentedMessage> e = consumer.ReceiveRentedAsync(cts.Token)
                         .GetAsyncEnumerator(TestContext.Current.CancellationToken))
        {
            for (var i = 0; i < 3; i++)
            {
                Assert.True(await e.MoveNextAsync());
                got.Add(e.Current);
            }
        }

        // Dispose only the first two: buffer must still be alive.
        got[0].Dispose();
        Assert.Equal(0, owner.DisposeCount);
        got[1].Dispose();
        Assert.Equal(0, owner.DisposeCount);

        // Final dispose returns the buffer.
        got[2].Dispose();
        Assert.Equal(1, owner.DisposeCount);

        // Calling Dispose again is a no-op — refcount must not drop below zero.
        got[0].Dispose();
        got[1].Dispose();
        got[2].Dispose();
        Assert.Equal(1, owner.DisposeCount);

        await consumer.DisposeAsync();
    }

    [Fact]
    public async Task ReceiveRentedAsync_Should_SurfaceDecryptionException_FromClient()
    {
        var client = new Mock<IIggyClient>(MockBehavior.Loose);
        client.Setup(c => c.ConnectAsync(It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
        client.Setup(c => c.PollMessagesRentedAsync(It.IsAny<Identifier>(), It.IsAny<Identifier>(),
                It.IsAny<uint?>(), It.IsAny<Consumer>(), It.IsAny<PollingStrategy>(), It.IsAny<uint>(),
                It.IsAny<bool>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new MessageDecryptionException(0, 1, new InvalidOperationException("decrypt fail")));
        var consumer = new IggyConsumer(client.Object, BuildConfig(), NullLoggerFactory.Instance);
        await consumer.InitAsync(TestContext.Current.CancellationToken);

        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await Assert.ThrowsAsync<MessageDecryptionException>(async () =>
        {
            await foreach (var _ in consumer.ReceiveRentedAsync(cts.Token))
            {
            }
        });

        await consumer.DisposeAsync();
    }

    [Fact]
    public async Task ReceiveRentedAsync_Should_Throw_WhenAutoCommitCombinedWithEncryptor()
    {
        var client = new Mock<IIggyClient>(MockBehavior.Loose);
        client.Setup(c => c.ConnectAsync(It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
        client.SetupGet(c => c.MessageEncryptor)
            .Returns(new AesMessageEncryptor(AesMessageEncryptor.GenerateKey()));

        var config = BuildConfig();
        config.AutoCommit = true;
        var consumer = new IggyConsumer(client.Object, config, NullLoggerFactory.Instance);
        await consumer.InitAsync(TestContext.Current.CancellationToken);

        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var _ in consumer.ReceiveRentedAsync(cts.Token))
            {
            }
        });
        Assert.Contains("AutoCommit", ex.Message);

        await consumer.DisposeAsync();
    }

    [Fact]
    public async Task ReceiveRentedAsync_Should_Throw_WhenConsumerNotInitialized()
    {
        Mock<IIggyClient> client = BuildClientMock(new Queue<PolledMessagesRental>());
        var consumer = new IggyConsumer(client.Object, BuildConfig(), NullLoggerFactory.Instance);

        await Assert.ThrowsAsync<ConsumerNotInitializedException>(async () =>
        {
            await foreach (var _ in consumer.ReceiveRentedAsync(TestContext.Current.CancellationToken))
            {
            }
        });

        await consumer.DisposeAsync();
    }

    [Fact]
    public void RentedBatchHandle_AcquireRelease_Balanced_DisposesOnce()
    {
        var owner = new TrackingMemoryOwner(16);
        var rental = new PolledMessagesRental(owner)
        {
            PartitionId = 1,
            CurrentOffset = 0,
            Messages = Array.Empty<RentedMessageResponse>()
        };
        var handle = new RentedBatchHandle(rental); // refCount=1 (self-ref)

        handle.Acquire(); // 2
        handle.Acquire(); // 3
        Assert.Equal(0, owner.DisposeCount);

        handle.Release(); // 2
        handle.Release(); // 1 (self-ref still alive)
        Assert.Equal(0, owner.DisposeCount);

        handle.Release(); // 0 -> disposed
        Assert.Equal(1, owner.DisposeCount);
    }

    [Fact]
    public void PolledMessagesRental_Dispose_Idempotent()
    {
        var owner = new TrackingMemoryOwner(16);
        var rental = new PolledMessagesRental(owner)
        {
            PartitionId = 1,
            CurrentOffset = 0,
            Messages = Array.Empty<RentedMessageResponse>()
        };

        rental.Dispose();
        rental.Dispose();
        rental.Dispose();

        Assert.Equal(1, owner.DisposeCount);
    }

    [Fact]
    public async Task PolledMessagesRental_ConcurrentDispose_ReturnsBufferOnce()
    {
        var owner = new TrackingMemoryOwner(16);
        var rental = new PolledMessagesRental(owner)
        {
            PartitionId = 1,
            CurrentOffset = 0,
            Messages = Array.Empty<RentedMessageResponse>()
        };

        using var start = new ManualResetEventSlim(false);
        Task[] tasks = Enumerable.Range(0, 64).Select(_ => Task.Run(() =>
        {
            start.Wait();
            rental.Dispose();
        })).ToArray();

        start.Set();
        await Task.WhenAll(tasks);

        Assert.Equal(1, owner.DisposeCount);
    }

    [Fact]
    public async Task PollRented_MidLoopPublishFailure_DoesNotLeakBuffer()
    {
        var owner = new TrackingMemoryOwner(1024);
        IReadOnlyList<RentedMessageResponse> messages = BuildMessages(owner, 5);
        var rental = new PolledMessagesRental(owner)
        {
            PartitionId = 1,
            CurrentOffset = 4,
            Messages = messages
        };
        Mock<IIggyClient> client = BuildClientMock(new Queue<PolledMessagesRental>(new[] { rental }));
        var consumer
            = new FailingPublishConsumer(client.Object, BuildConfig(), NullLoggerFactory.Instance) { FailAfter = 2 };
        await consumer.InitAsync(TestContext.Current.CancellationToken);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var got = new List<ReceivedRentedMessage>();
        IAsyncEnumerator<ReceivedRentedMessage> enumerator = consumer.ReceiveRentedAsync(cts.Token)
            .GetAsyncEnumerator(TestContext.Current.CancellationToken);

        for (var i = 0; i < 2; i++)
        {
            Assert.True(await enumerator.MoveNextAsync());
            got.Add(enumerator.Current);
        }

        // First 2 publishes succeeded, 3rd injected failure aborted the loop.
        // Producer self-ref was released in finally; consumer refs (2) still hold the buffer.
        Assert.Equal(2, got.Count);
        Assert.Equal(0, owner.DisposeCount);

        foreach (var m in got)
        {
            m.Dispose();
        }

        // All refs released -> rental disposed exactly once.
        Assert.Equal(1, owner.DisposeCount);

        cts.Cancel();
        try
        {
            await enumerator.DisposeAsync();
        }
        catch (OperationCanceledException)
        {
        }

        await consumer.DisposeAsync();
    }

    internal static IggyConsumerConfig BuildConfig()
    {
        return new IggyConsumerConfig
        {
            StreamId = Identifier.Numeric(1),
            TopicId = Identifier.Numeric(1),
            Consumer = Consumer.New(1),
            PollingStrategy = PollingStrategy.Next(),
            BatchSize = 10,
            PartitionId = 1,
            AutoCommitMode = AutoCommitMode.Disabled,
            AutoCommit = false,
            PollingIntervalMs = 0
        };
    }

    /// <summary>
    ///     Slices payload bytes into the supplied owner's memory and returns a list of
    ///     <see cref="RentedMessageResponse" /> instances backed by that single rented buffer.
    /// </summary>
    internal static IReadOnlyList<RentedMessageResponse> BuildMessages(TrackingMemoryOwner owner, int count)
    {
        var list = new List<RentedMessageResponse>(count);
        Memory<byte> buffer = owner.Memory;
        var written = 0;
        for (var i = 0; i < count; i++)
        {
            var bytes = Encoding.UTF8.GetBytes($"msg-{i}");
            bytes.CopyTo(buffer.Slice(written, bytes.Length));
            Memory<byte> slice = buffer.Slice(written, bytes.Length);
            written += bytes.Length;

            list.Add(new RentedMessageResponse
            {
                Header = new MessageHeader
                {
                    Offset = (ulong)i,
                    PayloadLength = bytes.Length
                },
                Payload = slice,
                RawUserHeaders = ReadOnlyMemory<byte>.Empty
            });
        }

        return list;
    }

    /// <summary>
    ///     Builds a <see cref="Mock{IIggyClient}" /> that dequeues rentals on each
    ///     <c>PollMessagesRentedAsync</c> call. When the queue is empty, returns an
    ///     empty rental so the consumer can spin without dereferencing null.
    /// </summary>
    internal static Mock<IIggyClient> BuildClientMock(Queue<PolledMessagesRental> rentals)
    {
        var mock = new Mock<IIggyClient>(MockBehavior.Loose);
        mock.Setup(c => c.ConnectAsync(It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
        mock.Setup(c => c.PollMessagesRentedAsync(It.IsAny<Identifier>(), It.IsAny<Identifier>(), It.IsAny<uint?>(),
                It.IsAny<Consumer>(), It.IsAny<PollingStrategy>(), It.IsAny<uint>(), It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(() =>
            {
                if (rentals.Count > 0)
                {
                    return rentals.Dequeue();
                }

                return new PolledMessagesRental(TcpMessageStream.EmptyMemoryOwner.Instance)
                {
                    PartitionId = 1,
                    CurrentOffset = 0,
                    Messages = Array.Empty<RentedMessageResponse>()
                };
            });
        return mock;
    }

    /// <summary>
    ///     <see cref="IMemoryOwner{T}" /> wrapper that counts how many times
    ///     <see cref="IDisposable.Dispose" /> has been invoked, so tests can assert that
    ///     the rental returns to the pool exactly once.
    /// </summary>
    internal sealed class TrackingMemoryOwner : IMemoryOwner<byte>
    {
        private readonly byte[] _buffer;

        public int DisposeCount { get; private set; }

        public TrackingMemoryOwner(int size)
        {
            _buffer = new byte[size];
        }

        public Memory<byte> Memory => _buffer;

        public void Dispose()
        {
            DisposeCount++;
        }
    }

    private sealed class FailingPublishConsumer : IggyConsumer
    {
        private int _calls;

        public int FailAfter { get; set; }

        public FailingPublishConsumer(IIggyClient client, IggyConsumerConfig config,
            ILoggerFactory loggerFactory)
            : base(client, config, loggerFactory)
        {
        }

        protected override async Task PublishRentedAsync(RentedBatchHandle rental, RentedMessageResponse message,
            uint partitionId, MessageStatus status, Exception? error, CancellationToken ct)
        {
            if (Interlocked.Increment(ref _calls) > FailAfter)
            {
                throw new InvalidOperationException("inject publish failure");
            }

            await base.PublishRentedAsync(rental, message, partitionId, status, error, ct);
        }
    }
}
