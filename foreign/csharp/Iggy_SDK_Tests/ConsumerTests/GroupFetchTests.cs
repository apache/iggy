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
using System.Collections.Concurrent;
using System.Security.Cryptography;
using Apache.Iggy.Configuration;
using Apache.Iggy.Consumers;
using Apache.Iggy.Contracts;
using Apache.Iggy.Encryption;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.IggyClient.Implementations;
using Apache.Iggy.Kinds;
using Apache.Iggy.Utils;
using Apache.Iggy.Vsr;
using Apache.Iggy.Tests.VsrTests;
using Microsoft.Extensions.Logging.Abstractions;
using static Apache.Iggy.Tests.VsrTests.MockFrames;

namespace Apache.Iggy.Tests.ConsumerTests;

/// <summary>
///     A consumer group member over TCP fetches its partitions with one request per primary (code 109). One node is
///     the primary of both partitions of the member, and each test answers the fetches, or parks one by not
///     answering it. Mirrors the fetch worker of core/sdk/src/clients/consumer/deferred.rs.
/// </summary>
public sealed class GroupFetchTests
{
    private const uint PartitionId = 3;
    private const uint OtherPartitionId = 4;

    /// <summary>The context that the routes report, before a fetch served a partition.</summary>
    private static readonly PartitionContext Route = new(17, 0, 50);

    /// <summary>The context that the node serves both partitions under.</summary>
    private static readonly PartitionContext Served = new(17, 9, 52);

    /// <summary>How long a negative check waits for a request or a completion that must not come.</summary>
    private static readonly TimeSpan SettleDelay = TimeSpan.FromMilliseconds(200);

    private static readonly TimeSpan PromptLimit = TimeSpan.FromSeconds(5);

    /// <summary>
    ///     The count of a fetch caps its whole reply, so a reply can serve only its first target. The next fetch
    ///     names the partitions served longest ago first, so each partition gets its turn.
    /// </summary>
    [Fact]
    public async Task given_a_fetch_that_serves_only_its_first_target_when_fetching_again_should_send_the_served_partition_last()
    {
        using var server = new FetchServer((request, fetch, _) =>
        {
            var first = fetch.Targets[0];
            return Reply(request.Operation, FetchBody(GoldenSection(first.PartitionId, first.Value, Served)));
        });
        using var client = Client(server);
        await using var consumer = await StartAsync(client, false, PollingStrategy.Offset(0));

        var received = await OffsetContinuationTests.ReceiveAsync(consumer, false, 6);

        Assert.Equal([(PartitionId, 0ul), (PartitionId, 1ul), (OtherPartitionId, 0ul), (OtherPartitionId, 1ul),
            (PartitionId, 2ul), (PartitionId, 3ul)], received.Select(message => (message.PartitionId, message.Offset)));
        Assert.Equal([
            [(PartitionId, 0ul), (OtherPartitionId, 0ul)],
            [(OtherPartitionId, 0ul), (PartitionId, 2ul)],
            [(PartitionId, 2ul), (OtherPartitionId, 2ul)]
        ], server.Fetches.Take(3).Select(Positions));
    }

    /// <summary>
    ///     The server resolves a Next target to the stored offset, and its section carries the next offset also
    ///     without messages. From then on the member sends the partition as that offset, under the context of the
    ///     section.
    /// </summary>
    [Fact]
    public async Task given_the_next_strategy_when_a_section_holds_no_messages_should_send_its_partition_as_an_offset()
    {
        using var server = new FetchServer((request, _, index) => index == 1
            ? Reply(request.Operation, FetchBody(EmptySection(PartitionId, 7, Served),
                GoldenSection(OtherPartitionId, 0, Served)))
            : null);
        using var client = Client(server);
        await using var consumer = await StartAsync(client, false, PollingStrategy.Next());

        var received = await OffsetContinuationTests.ReceiveAsync(consumer, false, 2);

        Assert.Equal([(OtherPartitionId, 0ul), (OtherPartitionId, 1ul)],
            received.Select(message => (message.PartitionId, message.Offset)));
        await UntilAsync(() => server.Fetches.Count == 2);
        Assert.Equal([
            [
                new FetchTarget(PartitionId, MessagePolling.Next, 0, 0, Route.Incarnation, Route.OwnerGeneration),
                new FetchTarget(OtherPartitionId, MessagePolling.Next, 0, 0, Route.Incarnation, Route.OwnerGeneration)
            ],
            [
                new FetchTarget(PartitionId, MessagePolling.Offset, 7, 0, Served.Incarnation, Served.OwnerGeneration),
                new FetchTarget(OtherPartitionId, MessagePolling.Offset, GOLDEN_BATCH_MESSAGES, 0, Served.Incarnation,
                    Served.OwnerGeneration)
            ]
        ], server.Fetches.Select(fetch => fetch.Targets.Select(target => target with { MaxBytes = 0 })));
    }

    /// <summary>
    ///     A refused section keeps the position of its partition. Not primary looks the route up again. A lost
    ///     ownership syncs the assignment again and starts the partition over from the configured strategy, under the
    ///     context of its route. Every refusal but not primary reaches the error events.
    /// </summary>
    [Theory]
    [InlineData(VsrError.NOT_PRIMARY)]
    [InlineData(VsrError.CONSUMER_GROUP_PARTITION_NOT_OWNED)]
    [InlineData(VsrError.TRANSIENT_NOT_ACCEPTED)]
    public async Task given_a_refused_section_when_fetching_again_should_keep_or_reset_its_partition_by_its_status(
        int status)
    {
        using var server = new FetchServer((request, _, index) => index switch
        {
            1 => Reply(request.Operation, FetchBody(GoldenSection(PartitionId, 0, Served),
                GoldenSection(OtherPartitionId, 0, Served))),
            2 => Reply(request.Operation, FetchBody(EmptySection(PartitionId, 0, default, (uint)status),
                GoldenSection(OtherPartitionId, GOLDEN_BATCH_MESSAGES, Served))),
            _ => null
        });
        using var client = Client(server);
        await using var consumer = await StartAsync(client, false, PollingStrategy.Offset(0));
        var errors = new ConcurrentQueue<Exception>();
        consumer.SubscribeToErrorEvents(error =>
        {
            errors.Enqueue(error.Exception);
            return Task.CompletedTask;
        });

        var received = await OffsetContinuationTests.ReceiveAsync(consumer, false, 6);

        Assert.Equal(6, received.Count);
        await UntilAsync(() => server.Fetches.Count == 3);
        var retried = Assert.Single(server.Fetches.ElementAt(2).Targets, target => target.PartitionId == PartitionId);
        var (offset, fence) = status == VsrError.CONSUMER_GROUP_PARTITION_NOT_OWNED
            ? (0ul, Route)
            : (GOLDEN_BATCH_MESSAGES, Served);
        Assert.Equal((MessagePolling.Offset, offset, fence.Incarnation, fence.OwnerGeneration),
            (retried.Kind, retried.Value, retried.Incarnation, retried.OwnerGeneration));
        Assert.Equal(status == VsrError.NOT_PRIMARY ? 2 : 1, server.RoutedPartitions.Count(id => id == PartitionId));
        Assert.Equal(status == VsrError.CONSUMER_GROUP_PARTITION_NOT_OWNED ? 2 : 1,
            server.Node.Requests(CommandCodes.SYNC_CONSUMER_GROUP_CODE));
        int[] reported = status == VsrError.NOT_PRIMARY ? [] : [status];
        Assert.Equal(reported, errors.Select(error => Assert.IsType<IggyInvalidStatusCodeException>(error).StatusCode));
    }

    /// <summary>
    ///     A record wider than the byte limit refuses its section, so the next poll reads that record alone, and the
    ///     fetches leave the partition out until then. The partition then joins its group in a second fetch beside
    ///     the parked one. When the second fetch replies first, its partition waits for the parked fetch, so the
    ///     next fetch names the whole group again.
    /// </summary>
    [Fact]
    public async Task given_an_oversized_record_when_it_was_read_alone_should_fetch_its_partition_beside_the_parked_fetch()
    {
        var parked = new TaskCompletionSource<MockRequest>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var server = new FetchServer((request, _, index) =>
        {
            switch (index)
            {
                case 1:
                    return Reply(request.Operation, FetchBody(
                        EmptySection(PartitionId, 0, default, VsrError.INVALID_SIZE_BYTES),
                        GoldenSection(OtherPartitionId, 0, Served)));
                case 2:
                    parked.TrySetResult(request);
                    return null;
                case 3:
                    return Reply(request.Operation, FetchBody(GoldenSection(PartitionId, GOLDEN_BATCH_MESSAGES, Served)));
                default:
                    return null;
            }
        });
        using var client = Client(server);
        await using var consumer = await StartAsync(client, false, PollingStrategy.Offset(0));
        var token = TestContext.Current.CancellationToken;

        var received = await OffsetContinuationTests.ReceiveAsync(consumer, false, 6);

        Assert.Equal([(OtherPartitionId, 0ul), (OtherPartitionId, 1ul), (PartitionId, 0ul), (PartitionId, 1ul),
            (PartitionId, 2ul), (PartitionId, 3ul)], received.Select(message => (message.PartitionId, message.Offset)));
        Assert.Equal((PartitionId, MessagePolling.Offset, 0ul, 1u), Assert.Single(server.Polls));
        await Task.Delay(SettleDelay, token);
        Assert.Equal([
            [(PartitionId, 0ul), (OtherPartitionId, 0ul)],
            [(OtherPartitionId, GOLDEN_BATCH_MESSAGES)],
            [(PartitionId, GOLDEN_BATCH_MESSAGES)]
        ], server.Fetches.Select(Positions));

        var request = await parked.Task.WaitAsync(PromptLimit, token);
        await server.Node.AnswerParkedAsync(request, Reply(request.Operation,
            FetchBody(GoldenSection(OtherPartitionId, GOLDEN_BATCH_MESSAGES, Served))));
        Assert.Equal(2, (await OffsetContinuationTests.ReceiveAsync(consumer, false, 2)).Count);
        await UntilAsync(() => server.Fetches.Count == 4);
        Assert.Equal([(PartitionId, 2 * GOLDEN_BATCH_MESSAGES), (OtherPartitionId, 2 * GOLDEN_BATCH_MESSAGES)],
            Positions(server.Fetches.ElementAt(3)));
    }

    /// <summary>
    ///     A fetch that commits nothing leaves each position to the client. A section that does not decrypt keeps
    ///     its position, so the next fetch reads it again, and the other sections are delivered before the failure.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task given_a_section_that_does_not_decrypt_when_fetched_should_read_it_again_after_the_others(
        bool rented)
    {
        using var server = new FetchServer((request, fetch, index) => index <= 2
            ? Reply(request.Operation, FetchBody([
                .. fetch.Targets.Select(target => GoldenSection(target.PartitionId, target.Value, Served))
            ]))
            : null);
        using var client = Client(server, new FirstDecryptionFails());
        await using var consumer = await StartAsync(client, false, PollingStrategy.Offset(0));
        var token = TestContext.Current.CancellationToken;

        var undecrypted = await Assert.ThrowsAsync<MessageDecryptionException>(async () =>
        {
            if (rented)
            {
                await foreach (var message in consumer.ReceiveRentedAsync(token))
                {
                    message.Dispose();
                }
            }
            else
            {
                await foreach (var _ in consumer.ReceiveAsync(token))
                {
                }
            }
        });
        var received = await OffsetContinuationTests.ReceiveAsync(consumer, rented, 6);

        Assert.Equal((PartitionId, 0ul), (undecrypted.PartitionId, undecrypted.Offset));
        Assert.Equal([(OtherPartitionId, 0ul), (OtherPartitionId, 1ul), (PartitionId, 0ul), (PartitionId, 1ul),
            (OtherPartitionId, 2ul), (OtherPartitionId, 3ul)], received.Select(message => (message.PartitionId,
            message.Offset)));
        Assert.Equal([(PartitionId, 0ul), (OtherPartitionId, GOLDEN_BATCH_MESSAGES)],
            Positions(server.Fetches.ElementAt(1)));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task given_a_malformed_section_when_a_later_fetch_is_in_flight_should_rewind_and_discard_its_later_section(
        bool rented)
    {
        var parked = new TaskCompletionSource<MockRequest>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var server = new FetchServer((request, fetch, index) =>
        {
            if (index == 2)
            {
                parked.TrySetResult(request);
                return null;
            }

            return index switch
            {
                1 => Reply(request.Operation, FetchBody(
                    GoldenSection(PartitionId, 0, Served) with { Records = [] },
                    GoldenSection(OtherPartitionId, 0, Served))),
                3 => Reply(request.Operation, FetchBody([
                    .. fetch.Targets.Select(target => GoldenSection(target.PartitionId, target.Value, Served))
                ])),
                _ => null
            };
        });
        using var client = Client(server);
        await using var consumer = await StartAsync(client, false, PollingStrategy.Offset(0));

        await Assert.ThrowsAsync<MalformedResponseException>(() =>
            OffsetContinuationTests.ReceiveAsync(consumer, rented, 1));
        var request = await parked.Task.WaitAsync(PromptLimit, TestContext.Current.CancellationToken);
        await server.Node.AnswerParkedAsync(request, Reply(request.Operation, FetchBody([
            .. ReadFetch(request).Targets.Select(target => GoldenSection(target.PartitionId, target.Value, Served))
        ])));
        var received = await OffsetContinuationTests.ReceiveAsync(consumer, rented, 8);

        Assert.Equal([(OtherPartitionId, 0ul), (OtherPartitionId, 1ul), (OtherPartitionId, 2ul),
            (OtherPartitionId, 3ul), (PartitionId, 0ul), (PartitionId, 1ul), (OtherPartitionId, 4ul),
            (OtherPartitionId, 5ul)], received.Select(message => (message.PartitionId, message.Offset)));
        Assert.Equal([(PartitionId, GOLDEN_BATCH_MESSAGES), (OtherPartitionId, GOLDEN_BATCH_MESSAGES)],
            Positions(server.Fetches.ElementAt(1)));
        var retried = Assert.Single(server.Fetches.ElementAt(2).Targets, target => target.PartitionId == PartitionId);
        Assert.Equal((0ul, Served.Incarnation, Served.OwnerGeneration),
            (retried.Value, retried.Incarnation, retried.OwnerGeneration));
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public async Task given_partial_uncertain_progress_when_receiving_should_preserve_the_healthy_sibling_and_its_rewind(
        bool uncertainFirst, bool malformed)
    {
        var healthy = GoldenSection(OtherPartitionId, 10, Served);
        if (malformed)
        {
            healthy = healthy with { Records = [] };
        }
        var uncertain = EmptySection(PartitionId, 0, default, VsrError.TRANSIENT_NOT_COMMITTED);
        using var server = new FetchServer((request, _, _) => Reply(request.Operation,
            FetchBody(uncertainFirst ? [uncertain, healthy] : [healthy, uncertain])));
        using var client = Client(server);
        var consumer = await StartAsync(client, true, PollingStrategy.Offset(0));
        var received = new List<(uint Partition, ulong Offset)>();
        try
        {
            var failure = await Assert.ThrowsAsync<VsrFetchProgressUncertainException>(async () =>
            {
                await foreach (var message in consumer.ReceiveAsync(TestContext.Current.CancellationToken))
                {
                    received.Add((message.PartitionId, message.CurrentOffset));
                }
            });
            Assert.NotNull(failure.OperationId);
            Assert.Equal(malformed ? [] : new[] { (OtherPartitionId, 10ul), (OtherPartitionId, 11ul) }, received);
            var shutdown = await Assert.ThrowsAsync<VsrFetchProgressUncertainException>(() =>
                consumer.DisposeAsync().AsTask().WaitAsync(PromptLimit, TestContext.Current.CancellationToken));
            Assert.Equal(failure.OperationId, shutdown.OperationId);
            Assert.DoesNotContain(server.Offsets, offset => offset.PartitionId == PartitionId);
            if (malformed)
            {
                Assert.Equal(new StoredOffset(OtherPartitionId, 9, Served), Assert.Single(server.Offsets));
            }
            Assert.Single(server.Fetches);
            Assert.Equal(0, server.Node.Requests(CommandCodes.CANCEL_DEFERRED_REQUEST_CODE));
        }
        finally
        {
            await consumer.DisposeAsync();
        }
    }

    [Fact]
    public async Task given_a_committing_fetch_when_shutdown_observes_uncertainty_should_report_recovery_without_rewind()
    {
        var parked = new TaskCompletionSource<MockRequest>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var server = new FetchServer((request, _, _) =>
        {
            parked.TrySetResult(request);
            return null;
        });
        using var client = Client(server);
        var consumer = await StartAsync(client, true, PollingStrategy.Next());
        var receiving = OffsetContinuationTests.ReceiveAsync(consumer, false, 1);
        var request = await parked.Task.WaitAsync(PromptLimit, TestContext.Current.CancellationToken);
        var disposing = consumer.DisposeAsync().AsTask();
        await server.Node.AnswerParkedAsync(request, Reply(request.Operation, [], VsrError.TRANSIENT_NOT_COMMITTED));
        var failure = await Assert.ThrowsAsync<VsrFetchProgressUncertainException>(() =>
            disposing.WaitAsync(PromptLimit, TestContext.Current.CancellationToken));
        Assert.NotNull(failure.OperationId);
        Assert.Empty(await receiving);
        Assert.Empty(server.Offsets);
        Assert.Equal(0, server.Node.Requests(CommandCodes.CANCEL_DEFERRED_REQUEST_CODE));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task given_a_committed_malformed_section_when_fetching_again_should_resume_at_its_first_offset(
        bool rented)
    {
        using var server = new FetchServer((request, fetch, index) => index switch
        {
            1 => Reply(request.Operation, FetchBody(
                GoldenSection(PartitionId, 0, Served) with { Records = [] },
                GoldenSection(OtherPartitionId, 0, Served))),
            2 => Reply(request.Operation, FetchBody([
                .. fetch.Targets.Select(target => GoldenSection(target.PartitionId, target.Value, Served))
            ])),
            _ => null
        });
        using var client = Client(server);
        await using var consumer = await StartAsync(client, true, PollingStrategy.Next());

        await Assert.ThrowsAsync<MalformedResponseException>(() =>
            OffsetContinuationTests.ReceiveAsync(consumer, rented, 1));
        var received = await OffsetContinuationTests.ReceiveAsync(consumer, rented, 6);

        Assert.Equal([(OtherPartitionId, 0ul), (OtherPartitionId, 1ul), (PartitionId, 0ul), (PartitionId, 1ul),
            (OtherPartitionId, 2ul), (OtherPartitionId, 3ul)],
            received.Select(message => (message.PartitionId, message.Offset)));
        var retried = Assert.Single(server.Fetches.ElementAt(1).Targets, target => target.PartitionId == PartitionId);
        Assert.Equal((MessagePolling.Offset, 0ul, Served.Incarnation, Served.OwnerGeneration),
            (retried.Kind, retried.Value, retried.Incarnation, retried.OwnerGeneration));
    }

    [Fact]
    public void given_a_group_consumer_with_only_the_reply_margin_when_constructed_should_refuse_it_before_startup()
    {
        using var server = new FetchServer((_, _, _) => null);
        using var client = Client(server);
        var options = new DeferredPollOptions { MaxWait = TimeSpan.Zero, RequestTimeout = TimeSpan.FromSeconds(1) };
        var config = new IggyConsumerConfig { Consumer = Consumer.Group("group"), PollOptions = options };

        var error = Assert.Throws<IggyInvalidStatusCodeException>(() =>
            new IggyConsumer(client, config, NullLoggerFactory.Instance));

        Assert.Equal(VsrError.INVALID_COMMAND, error.StatusCode);
        Assert.Empty(server.Fetches);
    }

    /// <summary>
    ///     Dispose cancels a parked fetch that commits nothing. A committing fetch can commit before a cancel
    ///     arrives, so dispose waits for its reply instead. The application never takes those messages, so dispose
    ///     then moves the stored offset back before them under the context of the fetch: to the offset before the
    ///     first one, or deleted when the first one is offset zero.
    /// </summary>
    [Theory]
    [InlineData(false, MessagePolling.Next)]
    [InlineData(true, MessagePolling.Next)]
    [InlineData(false, MessagePolling.Offset)]
    [InlineData(true, MessagePolling.Offset)]
    public async Task given_a_parked_fetch_when_the_consumer_is_disposed_should_cancel_it_unless_it_commits(
        bool autoCommit, MessagePolling strategy)
    {
        var parked = new TaskCompletionSource<MockRequest>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var server = new FetchServer((request, _, _) =>
        {
            parked.TrySetResult(request);
            return null;
        });
        using var client = Client(server);
        await using var consumer = await StartAsync(client, autoCommit,
            strategy == MessagePolling.Next ? PollingStrategy.Next() : PollingStrategy.Offset(0));
        var token = TestContext.Current.CancellationToken;
        var receiving = OffsetContinuationTests.ReceiveAsync(consumer, false, 1);
        var request = await parked.Task.WaitAsync(PromptLimit, token);

        var disposing = consumer.DisposeAsync().AsTask();

        if (!autoCommit)
        {
            await disposing.WaitAsync(PromptLimit, token);
            await UntilAsync(() => server.Node.Requests(CommandCodes.CANCEL_DEFERRED_REQUEST_CODE) == 1);
            Assert.Empty(await receiving);
            Assert.Empty(server.Offsets);
            return;
        }

        await Task.Delay(SettleDelay, token);
        Assert.False(disposing.IsCompleted);
        Assert.Equal(0, server.Node.Requests(CommandCodes.CANCEL_DEFERRED_REQUEST_CODE));
        await server.Node.AnswerParkedAsync(request, Reply(request.Operation,
            FetchBody(GoldenSection(PartitionId, 0, Served), GoldenSection(OtherPartitionId, 5, Served))));
        await disposing.WaitAsync(PromptLimit, token);
        Assert.Empty(await receiving);
        Assert.Equal([new StoredOffset(PartitionId, null, Served), new StoredOffset(OtherPartitionId, 4, Served)],
            server.Offsets.OrderBy(stored => stored.PartitionId));
    }

    /// <summary>
    ///     A committing fetch commits also the messages that wait in the receive buffer at dispose, so dispose moves
    ///     the stored offset back to the last message that the application took.
    /// </summary>
    [Fact]
    public async Task given_a_committing_consumer_when_disposed_with_undelivered_messages_should_store_the_last_delivered_offset()
    {
        using var server = new FetchServer((request, _, index) => index == 1
            ? Reply(request.Operation, FetchBody(GoldenSection(PartitionId, 0, Served),
                GoldenSection(OtherPartitionId, 5, Served)))
            : null);
        using var client = Client(server);
        var consumer = await StartAsync(client, true, PollingStrategy.Next());

        var received = await OffsetContinuationTests.ReceiveAsync(consumer, false, 1);
        await consumer.DisposeAsync().AsTask().WaitAsync(PromptLimit, TestContext.Current.CancellationToken);

        Assert.Equal([(PartitionId, 0ul, Served)], received);
        Assert.True(Assert.Single(server.Fetches).AutoCommit);
        Assert.Equal([new StoredOffset(PartitionId, 0, Served), new StoredOffset(OtherPartitionId, 4, Served)],
            server.Offsets.OrderBy(stored => stored.PartitionId));
    }

    private static (uint PartitionId, ulong Offset)[] Positions(FetchRequest fetch)
    {
        return [.. fetch.Targets.Select(target => (target.PartitionId, target.Value))];
    }

    private static TcpMessageStream Client(FetchServer server, IMessageEncryptor? encryptor = null)
    {
        return new TcpMessageStream(new IggyClientConfigurator
        {
            BaseAddress = $"127.0.0.1:{server.Port}",
            Protocol = Protocol.Tcp,
            HeartbeatInterval = TimeSpan.FromHours(1),
            MessageEncryptor = encryptor
        }, NullLoggerFactory.Instance);
    }

    private static async Task<IggyConsumer> StartAsync(TcpMessageStream client, bool autoCommit,
        PollingStrategy strategy)
    {
        var started = new IggyConsumer(client, new IggyConsumerConfig
        {
            StreamId = Identifier.Numeric(1),
            TopicId = Identifier.Numeric(2),
            Consumer = Consumer.Group("group"),
            PollingStrategy = strategy,
            BatchSize = 10,
            AutoCommit = autoCommit,
            AutoCommitMode = autoCommit ? AutoCommitMode.Auto : AutoCommitMode.Disabled,
            Login = "iggy",
            Password = "iggy"
        }, NullLoggerFactory.Instance);
        await started.InitAsync(TestContext.Current.CancellationToken);

        return started;
    }

    private static async Task UntilAsync(Func<bool> predicate)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        timeout.CancelAfter(PromptLimit);
        while (!predicate())
        {
            await Task.Delay(5, timeout.Token);
        }
    }

    /// <summary>Copies each message as is, and fails the first decryption.</summary>
    private sealed class FirstDecryptionFails : IMessageEncryptor
    {
        private int _decryptions;

        public int GetMaxEncryptedLength(int plaintextLength) => plaintextLength;

        public int GetMaxDecryptedLength(int encryptedLength) => encryptedLength;

        public int Encrypt(ReadOnlySpan<byte> plaintext, Span<byte> destination)
        {
            plaintext.CopyTo(destination);
            return plaintext.Length;
        }

        public int Decrypt(ReadOnlySpan<byte> encryptedData, Span<byte> destination)
        {
            if (Interlocked.Increment(ref _decryptions) == 1)
            {
                throw new CryptographicException("The first message does not decrypt.");
            }

            encryptedData.CopyTo(destination);
            return encryptedData.Length;
        }
    }

    /// <summary>An offset that the member stored, or deleted with a null offset, and the context it sent.</summary>
    private readonly record struct StoredOffset(uint PartitionId, ulong? Offset, PartitionContext Context);

    /// <summary>
    ///     A standalone node that assigns partitions 3 and 4 to the member, routes both under <see cref="Route" />,
    ///     and passes each fetch to the handler with its 1-based number. It serves a poll with the golden batch from
    ///     where the poll asks, never answers a cancel, and records the fetches, the polls, the route lookups and the
    ///     offsets that the member stores or deletes.
    /// </summary>
    private sealed class FetchServer : IDisposable
    {
        /// <summary>A StoreConsumerOffset body ends with the partition id, the offset and the ack quorum.</summary>
        private const int StoredPartitionFromEnd = sizeof(uint) + sizeof(ulong) + sizeof(byte);

        /// <summary>A DeleteConsumerOffset body ends with the partition id and the ack quorum.</summary>
        private const int DeletedPartitionFromEnd = sizeof(uint) + sizeof(byte);

        private readonly Func<MockRequest, FetchRequest, int, byte[]?> _fetch;
        private int _fetches;

        internal FetchServer(Func<MockRequest, FetchRequest, int, byte[]?> fetch)
        {
            _fetch = fetch;
            Node.Serve(Handle);
        }

        internal MockNode Node { get; } = new();

        internal ushort Port => Node.Port;

        internal ConcurrentQueue<FetchRequest> Fetches { get; } = new();

        internal ConcurrentQueue<(uint PartitionId, MessagePolling Kind, ulong Value, uint Count)> Polls { get; } =
            new();

        internal ConcurrentQueue<uint> RoutedPartitions { get; } = new();

        internal ConcurrentQueue<StoredOffset> Offsets { get; } = new();

        public void Dispose()
        {
            Node.Dispose();
        }

        private byte[]? Handle(MockRequest request)
        {
            switch ((VsrOperation)request.Operation)
            {
                case VsrOperation.StoreConsumerOffset:
                    Offsets.Enqueue(new StoredOffset(
                        BinaryPrimitives.ReadUInt32LittleEndian(request.Body.AsSpan(^StoredPartitionFromEnd)),
                        BinaryPrimitives.ReadUInt64LittleEndian(
                            request.Body.AsSpan(^(StoredPartitionFromEnd - sizeof(uint)))), request.Context));
                    return Answer(request);
                case VsrOperation.DeleteConsumerOffset:
                    Offsets.Enqueue(new StoredOffset(
                        BinaryPrimitives.ReadUInt32LittleEndian(request.Body.AsSpan(^DeletedPartitionFromEnd)), null,
                        request.Context));
                    return Answer(request);
            }

            switch (request.Code)
            {
                case GET_CLUSTER_METADATA_CODE:
                    return StandaloneRoster(Node.Port);
                case CommandCodes.GET_PARTITION_ROUTE_CODE:
                    RoutedPartitions.Enqueue(
                        BinaryPrimitives.ReadUInt32LittleEndian(request.Body.AsSpan(^sizeof(uint))));
                    return RouteReply(request, Route, 0, 2);
                case CommandCodes.SYNC_CONSUMER_GROUP_CODE:
                    return Reply(OPERATION_NON_REPLICATED, AssignmentBody(1, [PartitionId, OtherPartitionId]));
                case CommandCodes.FETCH_MESSAGES_CODE:
                    var fetch = ReadFetch(request);
                    Fetches.Enqueue(fetch);
                    return _fetch(request, fetch, Interlocked.Increment(ref _fetches));
                case CommandCodes.POLL_MESSAGES_DEFERRED_CODE:
                    var poll = ReadPoll(request);
                    Polls.Enqueue(poll);
                    return Reply(request.Operation, GoldenPollBody(poll.PartitionId, poll.Value, Served));
                case CommandCodes.CANCEL_DEFERRED_REQUEST_CODE:
                    return null;
                default:
                    return Answer(request);
            }
        }
    }
}
