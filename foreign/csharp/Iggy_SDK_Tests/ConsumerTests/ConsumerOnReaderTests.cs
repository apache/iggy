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

using System.Collections.Concurrent;
using System.Text;
using Apache.Iggy.Consumers;
using Apache.Iggy.Contracts;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.IggyClient.Implementations;
using Apache.Iggy.Kinds;
using Apache.Iggy.Vsr;
using Microsoft.Extensions.Logging.Abstractions;

namespace Apache.Iggy.Tests.ConsumerTests;

/// <summary>
///     The consumer hands over one message at a time on a partition reader, and counts each message for an
///     automatic commit policy where its <see cref="CommitAfter" /> says.
/// </summary>
public sealed class ConsumerOnReaderTests
{
    private static readonly Identifier Stream = Identifier.Numeric(1);
    private static readonly Identifier Topic = Identifier.Numeric(2);
    private static readonly TimeSpan PromptLimit = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan RetryInterval = TimeSpan.FromMilliseconds(20);

    private static CancellationToken Token => TestContext.Current.CancellationToken;

    [Fact]
    public async Task given_a_standalone_consumer_of_two_partitions_when_receiving_should_hand_over_every_message()
    {
        using var cluster = new ReaderCluster(2);
        using var client = await cluster.ConnectAsync();
        await using var consumer = await StartAsync(client, CommitPolicy.Manual, 1, 2);

        var received = await ReceiveAsync(consumer, 4);

        Assert.Equal([(1u, 0ul), (1u, 1ul), (2u, 0ul), (2u, 1ul)],
            received.Select(message => (message.PartitionId, message.CurrentOffset)).Order());
        Assert.All(received, message => Assert.Equal(message.CurrentOffset, message.Checkpoint!.Offset));
    }

    [Fact]
    public async Task given_manual_commits_when_committing_a_received_message_should_store_its_offset()
    {
        using var cluster = new ReaderCluster(1);
        using var client = await cluster.ConnectAsync();
        await using var consumer = await StartAsync(client, CommitPolicy.Manual, 1);
        var received = await ReceiveAsync(consumer, 1);

        await consumer.CommitAsync(received[0].Checkpoint!, Token);

        Assert.Equal(0ul, cluster.Stored[1]);
    }

    /// <summary>A message counts once the application reads the stream again, and the last one on shutdown.</summary>
    [Fact]
    public async Task given_auto_commit_after_processing_when_reading_the_stream_again_should_count_the_previous_message()
    {
        using var cluster = new ReaderCluster(1);
        using var client = await cluster.ConnectAsync();
        var consumer = await StartAsync(client,
            CommitPolicy.Auto(new AutoCommit { Every = CommitEvery.Message, After = CommitAfter.Processing }), 1);
        await using var messages = consumer.ReceiveAsync(Token).GetAsyncEnumerator(Token);

        Assert.True(await messages.MoveNextAsync());
        await Task.Delay(TimeSpan.FromMilliseconds(200), Token);
        Assert.False(cluster.Stored.ContainsKey(1));
        Assert.True(await messages.MoveNextAsync());
        await UntilAsync(() => cluster.Stored.TryGetValue(1, out var stored) && stored == 0);
        await consumer.ShutdownAsync();

        Assert.Equal(1ul, cluster.Stored[1]);
    }

    [Fact]
    public async Task given_auto_commit_after_delivery_when_a_message_is_handed_over_should_count_it_at_once()
    {
        using var cluster = new ReaderCluster(1);
        using var client = await cluster.ConnectAsync();
        await using var consumer = await StartAsync(client,
            CommitPolicy.Auto(new AutoCommit { Every = CommitEvery.Message, After = CommitAfter.Delivery }), 1);
        await using var messages = consumer.ReceiveAsync(Token).GetAsyncEnumerator(Token);

        Assert.True(await messages.MoveNextAsync());

        await UntilAsync(() => cluster.Stored.TryGetValue(1, out var stored) && stored == 0);
    }

    /// <summary>A handler that fails ends the loop with its error, and its message never counts.</summary>
    [Fact]
    public async Task given_auto_commit_after_processing_when_a_handler_fails_should_end_the_loop_without_counting_it()
    {
        using var cluster = new ReaderCluster(1);
        using var client = await cluster.ConnectAsync();
        var consumer = await StartAsync(client,
            CommitPolicy.Auto(new AutoCommit { Interval = TimeSpan.FromHours(1), After = CommitAfter.Processing }), 1);
        var failure = new InvalidOperationException("The handler failed.");

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => consumer.ConsumeAsync(
            (message, _) => message.CurrentOffset == 1 ? Task.FromException(failure) : Task.CompletedTask, Token));
        await consumer.ShutdownAsync();

        Assert.Same(failure, error);
        Assert.Equal(0ul, cluster.Stored[1]);
    }

    /// <summary>
    ///     A shutdown that starts while a handler runs waits for it, because the message counts only once its handler
    ///     returns with success. This handler fails, so only the message before it is stored.
    /// </summary>
    [Fact]
    public async Task given_auto_commit_after_processing_when_shutting_down_during_a_handler_should_not_count_its_message()
    {
        using var cluster = new ReaderCluster(1);
        using var client = await cluster.ConnectAsync();
        var consumer = await StartAsync(client,
            CommitPolicy.Auto(new AutoCommit { Interval = TimeSpan.FromHours(1), After = CommitAfter.Processing }), 1);
        var failure = new InvalidOperationException("The handler failed.");
        var handling = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var consuming = consumer.ConsumeAsync(async (message, _) =>
        {
            if (message.CurrentOffset == 1)
            {
                handling.SetResult();
                await release.Task;
                throw failure;
            }
        }, Token);
        await handling.Task.WaitAsync(PromptLimit, Token);

        var shutdown = consumer.ShutdownAsync();
        await Task.WhenAny(shutdown, Task.Delay(TimeSpan.FromMilliseconds(200), Token));
        var waited = !shutdown.IsCompleted;
        release.SetResult();
        await shutdown;

        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => consuming));
        Assert.True(waited);
        Assert.Equal(0ul, cluster.Stored[1]);
    }

    /// <summary>
    ///     The first shutdown waits for the handler that runs. A second shutdown waits for the first instead of
    ///     returning while it still runs.
    /// </summary>
    [Fact]
    public async Task given_a_shutdown_in_progress_when_shutting_down_again_should_wait_for_the_first()
    {
        using var cluster = new ReaderCluster(1);
        using var client = await cluster.ConnectAsync();
        var consumer = await StartAsync(client,
            CommitPolicy.Auto(new AutoCommit { Interval = TimeSpan.FromHours(1), After = CommitAfter.Processing }), 1);
        var handling = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var consuming = consumer.ConsumeAsync(async (message, _) =>
        {
            if (message.CurrentOffset == 1)
            {
                handling.SetResult();
                await release.Task;
            }
        }, Token);
        await handling.Task.WaitAsync(PromptLimit, Token);
        var first = consumer.ShutdownAsync();

        var second = consumer.ShutdownAsync();
        await Task.WhenAny(second, Task.Delay(TimeSpan.FromMilliseconds(200), Token));
        var waited = !second.IsCompleted;
        release.SetResult();
        await first.WaitAsync(PromptLimit, Token);
        await second.WaitAsync(PromptLimit, Token);
        await consuming.WaitAsync(PromptLimit, Token);

        Assert.True(waited);
        Assert.Equal(1ul, cluster.Stored[1]);
    }

    /// <summary>
    ///     With on read commits the server stored both messages of the read, but the application took only the
    ///     first, so shutdown moves the offset back to it.
    /// </summary>
    [Fact]
    public async Task given_on_read_when_shutting_down_after_part_of_a_batch_should_move_the_offset_back_to_the_last_message_taken()
    {
        using var cluster = new ReaderCluster(1);
        using var client = await cluster.ConnectAsync();
        var consumer = await StartAsync(client, CommitPolicy.OnRead, 1);
        await using var messages = consumer.ReceiveAsync(Token).GetAsyncEnumerator(Token);
        Assert.True(await messages.MoveNextAsync());
        Assert.Equal(1ul, cluster.Stored[1]);

        await consumer.ShutdownAsync();

        Assert.Equal(0ul, cluster.Stored[1]);
    }

    [Fact]
    public async Task given_a_message_that_does_not_decode_when_receiving_should_end_the_stream_with_its_skip()
    {
        using var cluster = new ReaderCluster(1) { Limit = 5 };
        cluster.Undecodable.Add(2);
        using var client = await cluster.ConnectAsync();
        await using var consumer = await StartAsync(client, CommitPolicy.Manual, 1);
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(Token);
        timeout.CancelAfter(PromptLimit);
        List<ulong> offsets = [];

        var error = await Assert.ThrowsAsync<ReadException>(async () =>
        {
            await foreach (var message in consumer.ReceiveAsync(timeout.Token))
            {
                offsets.Add(message.CurrentOffset);
            }
        });

        Assert.Equal([0ul, 1ul], offsets);
        Assert.Equal((MessagePolling.Offset, 3ul), (error.Skip!.Value.Kind, error.Skip.Value.Value));
    }

    /// <summary>
    ///     Six refusals in a row stop the partition in the reader. The consumer seeks it to Next, so it reads the
    ///     partition again after its stored offset once the refusal clears, without a restart.
    /// </summary>
    [Fact]
    public async Task given_lasting_refusals_that_stop_a_partition_when_the_refusal_clears_should_read_it_again()
    {
        using var cluster = new ReaderCluster(1);
        cluster.Unauthorized.Add(1);
        using var client = await cluster.ConnectAsync();
        await using var consumer = await StartAsync(client, CommitPolicy.Manual, 1);
        ConcurrentBag<Exception> errors = [];
        consumer.SubscribeToErrorEvents(error =>
        {
            errors.Add(error.Exception);
            cluster.Unauthorized.Clear();
            return Task.CompletedTask;
        });

        var received = await ReceiveAsync(consumer, 2);

        var error = Assert.IsType<ReadException>(Assert.Single(errors));
        Assert.Equal((1u, VsrError.UNAUTHORIZED, true, true), (error.PartitionId,
            Assert.IsType<IggyInvalidStatusCodeException>(error.InnerException).StatusCode, error.StopsPartition,
            error.IsResumable));
        Assert.Equal([0ul, 1ul], received.Select(message => message.CurrentOffset));
    }

    /// <summary>
    ///     The history of the position is gone, so the reader stops the partition. The consumer seeks it to Next, so
    ///     it reads on after the last committed message.
    /// </summary>
    [Fact]
    public async Task given_a_lost_history_that_stops_a_partition_when_receiving_should_read_on_after_the_stored_offset()
    {
        using var cluster = new ReaderCluster(1) { Limit = 4 };
        using var client = await cluster.ConnectAsync();
        await using var consumer = await StartAsync(client, CommitPolicy.Manual, 1);
        ConcurrentBag<Exception> errors = [];
        consumer.SubscribeToErrorEvents(error =>
        {
            errors.Add(error.Exception);
            cluster.HistoryRefused.Clear();
            return Task.CompletedTask;
        });

        var resumed = await ReceiveAfterLostHistoryAsync(consumer, cluster, errors);

        var error = Assert.IsType<ReadException>(Assert.Single(errors));
        Assert.Equal((1u, VsrError.HISTORY_UNAVAILABLE, true, true), (error.PartitionId,
            Assert.IsType<IggyInvalidStatusCodeException>(error.InnerException).StatusCode, error.StopsPartition,
            error.IsResumable));
        Assert.Equal(1ul, resumed);
    }

    /// <summary>
    ///     An error handler seeks the stopped partition to an offset of its choice before its first await. The
    ///     consumer leaves that seek in place instead of seeking the partition to Next.
    /// </summary>
    [Fact]
    public async Task given_an_error_handler_that_seeks_a_stopped_partition_when_receiving_should_read_from_its_seek()
    {
        using var cluster = new ReaderCluster(1) { Limit = 4 };
        using var client = await cluster.ConnectAsync();
        await using var consumer = await StartAsync(client, CommitPolicy.Manual, 1);
        ConcurrentBag<Exception> errors = [];
        consumer.SubscribeToErrorEvents(error =>
        {
            errors.Add(error.Exception);
            cluster.HistoryRefused.Clear();
            return consumer.SeekAsync(1, PollingStrategy.Offset(3), Token);
        });

        var resumed = await ReceiveAfterLostHistoryAsync(consumer, cluster, errors);

        Assert.Single(errors);
        Assert.Equal(3ul, resumed);
    }

    /// <summary>
    ///     An error handler awaits before it seeks the stopped partition. The consumer waits for the handler, so that
    ///     seek stays in place too.
    /// </summary>
    [Fact]
    public async Task given_an_error_handler_that_seeks_a_stopped_partition_after_an_await_when_receiving_should_read_from_its_seek()
    {
        using var cluster = new ReaderCluster(1) { Limit = 4 };
        using var client = await cluster.ConnectAsync();
        await using var consumer = await StartAsync(client, CommitPolicy.Manual, 1);
        ConcurrentBag<Exception> errors = [];
        consumer.SubscribeToErrorEvents(async error =>
        {
            errors.Add(error.Exception);
            await Task.Delay(RetryInterval, Token);
            cluster.HistoryRefused.Clear();
            await consumer.SeekAsync(1, PollingStrategy.Offset(3), Token);
        });

        var resumed = await ReceiveAfterLostHistoryAsync(consumer, cluster, errors);

        Assert.Single(errors);
        Assert.Equal(3ul, resumed);
    }

    /// <summary>
    ///     The first message of a last start is above the byte limit of a read, so the reader stops the partition
    ///     until a seek that only the application can choose. The stream ends with the error, as it does for a message
    ///     that does not decode, instead of waiting on a partition that never reads again.
    /// </summary>
    [Fact]
    public async Task given_a_last_start_with_an_oversized_first_message_when_receiving_should_end_the_stream_with_its_stop()
    {
        using var cluster = new ReaderCluster(1) { Limit = 4, OversizedLast = true };
        using var client = await cluster.ConnectAsync();
        await using var consumer = new IggyConsumer(client, new IggyConsumerConfig
        {
            StreamId = Stream,
            TopicId = Topic,
            Consumer = Consumer.New(4),
            Partitions = [1],
            PollingStrategy = PollingStrategy.Last(),
            PollOptions = new PollOptions { MaxWait = TimeSpan.FromMilliseconds(100) },
            RetryInterval = RetryInterval
        }, NullLoggerFactory.Instance);
        await consumer.InitAsync(Token);
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(Token);
        timeout.CancelAfter(PromptLimit);

        var error = await Assert.ThrowsAsync<ReadException>(async () =>
        {
            await foreach (var _ in consumer.ReceiveAsync(timeout.Token))
            {
            }
        });

        Assert.Equal((1u, VsrError.INVALID_SIZE_BYTES, true, false), (error.PartitionId,
            Assert.IsType<IggyInvalidStatusCodeException>(error.InnerException).StatusCode, error.StopsPartition,
            error.IsResumable));
    }

    [Fact]
    public async Task given_a_cancel_after_a_message_when_receiving_should_stop_without_an_error()
    {
        using var cluster = new ReaderCluster(1) { Limit = 4 };
        using var client = await cluster.ConnectAsync();
        await using var consumer = await StartAsync(client, CommitPolicy.Manual, 1);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Token);
        var received = 0;

        await foreach (var _ in consumer.ReceiveAsync(cancellation.Token))
        {
            received++;
            await cancellation.CancelAsync();
        }

        Assert.Equal(1, received);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task given_a_handler_that_stops_consuming_when_it_returns_should_not_receive_another_message(bool shutdown)
    {
        using var cluster = new ReaderCluster(1) { Limit = 4 };
        using var client = await cluster.ConnectAsync();
        await using var consumer = await StartAsync(client, CommitPolicy.OnRead, 1);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Token);
        List<ulong> received = [];

        await consumer.ConsumeAsync(async (message, _) =>
        {
            received.Add(message.CurrentOffset);
            if (shutdown)
            {
                await consumer.ShutdownAsync();
            }
            else
            {
                await cancellation.CancelAsync();
            }
        }, cancellation.Token);
        await consumer.ShutdownAsync();

        Assert.Equal([0ul], received);
        Assert.Equal(0ul, cluster.Stored[1]);
    }

    [Fact]
    public async Task given_a_typed_consumer_when_receiving_should_deserialize_each_payload()
    {
        using var cluster = new ReaderCluster(1);
        using var client = await cluster.ConnectAsync();
        await using var consumer = new IggyConsumer<string>(client, new IggyConsumerConfig<string>
        {
            StreamId = Stream,
            TopicId = Topic,
            Consumer = Consumer.New(4),
            Partitions = [1],
            PollOptions = new PollOptions { MaxWait = TimeSpan.FromMilliseconds(100) },
            Deserializer = new Utf8Deserializer()
        }, NullLoggerFactory.Instance);
        await consumer.InitAsync(Token);
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(Token);
        timeout.CancelAfter(PromptLimit);

        List<string?> payloads = [];
        await foreach (var message in consumer.ReceiveDeserializedAsync(timeout.Token))
        {
            payloads.Add(message.Data);
            if (payloads.Count == 2)
            {
                break;
            }
        }

        Assert.Equal(["first-payload", "second-payload"], payloads);
    }

    [Fact]
    public async Task given_rented_messages_when_receiving_should_keep_the_batch_until_each_message_is_disposed()
    {
        using var cluster = new ReaderCluster(1);
        using var client = await cluster.ConnectAsync();
        await using var consumer = await StartAsync(client, CommitPolicy.Manual, 1);
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(Token);
        timeout.CancelAfter(PromptLimit);
        List<ReceivedRentedMessage> received = [];

        await foreach (var message in consumer.ReceiveRentedAsync(timeout.Token))
        {
            received.Add(message);
            if (received.Count == 2)
            {
                break;
            }
        }

        Assert.Equal("second-payload", Encoding.UTF8.GetString(received[1].Message.Payload.Span));
        Assert.Equal([0ul, 1ul], received.Select(message => message.Checkpoint!.Offset));
        foreach (var message in received)
        {
            message.Dispose();
        }
    }

    private static async Task<IggyConsumer> StartAsync(TcpMessageStream client, CommitPolicy commit,
        params uint[] partitions)
    {
        var consumer = new IggyConsumer(client, new IggyConsumerConfig
        {
            StreamId = Stream,
            TopicId = Topic,
            Consumer = Consumer.New(4),
            Partitions = partitions,
            PollOptions = new PollOptions { MaxWait = TimeSpan.FromMilliseconds(100), Commit = commit },
            RetryInterval = RetryInterval
        }, NullLoggerFactory.Instance);
        await consumer.InitAsync(Token);
        return consumer;
    }

    private static async Task<List<ReceivedMessage>> ReceiveAsync(IggyConsumer consumer, int count)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(Token);
        timeout.CancelAfter(PromptLimit);
        List<ReceivedMessage> received = [];
        await foreach (var message in consumer.ReceiveAsync(timeout.Token))
        {
            received.Add(message);
            if (received.Count == count)
            {
                break;
            }
        }

        return received;
    }

    /// <summary>
    ///     Commits offset 0 of partition 1, then loses the history of its position, and returns the first offset
    ///     received after the error, or null when none arrives in time.
    /// </summary>
    private static async Task<ulong?> ReceiveAfterLostHistoryAsync(IggyConsumer consumer, ReaderCluster cluster,
        ConcurrentBag<Exception> errors)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(Token);
        timeout.CancelAfter(PromptLimit);
        await foreach (var message in consumer.ReceiveAsync(timeout.Token))
        {
            if (message.CurrentOffset == 0)
            {
                await consumer.CommitAsync(message.Checkpoint!, Token);
                cluster.HistoryRefused.Add(1);
            }
            else if (!errors.IsEmpty)
            {
                return message.CurrentOffset;
            }
        }

        return null;
    }

    private static async Task UntilAsync(Func<bool> predicate)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(Token);
        timeout.CancelAfter(PromptLimit);
        while (!predicate())
        {
            await Task.Delay(5, timeout.Token);
        }
    }

    private sealed class Utf8Deserializer : IDeserializer<string>
    {
        public string Deserialize(ReadOnlyMemory<byte> data)
        {
            return Encoding.UTF8.GetString(data.Span);
        }
    }
}
