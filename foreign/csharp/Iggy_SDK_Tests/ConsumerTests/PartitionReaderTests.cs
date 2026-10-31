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
using Apache.Iggy.Configuration;
using Apache.Iggy.Consumers;
using Apache.Iggy.Contracts;
using Apache.Iggy.Contracts.Tcp;
using Apache.Iggy.Encryption;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Extensions;
using Apache.Iggy.IggyClient;
using Apache.Iggy.IggyClient.Implementations;
using Apache.Iggy.Messages;
using Apache.Iggy.Kinds;
using Apache.Iggy.Tests.VsrTests;
using Apache.Iggy.Utils;
using Apache.Iggy.Vsr;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using static Apache.Iggy.Tests.VsrTests.MockFrames;

namespace Apache.Iggy.Tests.ConsumerTests;

/// <summary>
///     The partition reader against a fake cluster whose nodes serve a log of <see cref="ReaderCluster.Limit" />
///     messages per partition, two per read, store and delete consumer offsets, and park a read that finds nothing
///     until its cancel, which they answer as the server does for a committing read.
/// </summary>
public sealed class PartitionReaderTests
{
    private static readonly Identifier Stream = Identifier.Numeric(1);
    private static readonly Identifier Topic = Identifier.Numeric(2);
    private static readonly TimeSpan PromptLimit = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan RetryInterval = TimeSpan.FromMilliseconds(50);

    /// <summary>A lost reply drops the connection, so what follows it waits for the client to connect again.</summary>
    private static readonly TimeSpan LostReplyLimit = TimeSpan.FromSeconds(30);

    [Fact]
    public async Task given_partitions_of_two_primaries_when_reading_should_fetch_each_primary_on_its_own()
    {
        using var cluster = new ReaderCluster(2);
        using var client = await cluster.ConnectAsync();
        await using var reader = await Reader(client, CommitPolicy.Manual, 0, 1, 2, 3).BuildAsync(Token);

        var delivered = await ReadAsync(reader, 4);

        Assert.Equal([0u, 1u, 2u, 3u], delivered.Select(batch => batch.PartitionId).Order());
        Assert.All(delivered, batch => Assert.Equal([0ul, 1ul], batch.Messages.Select(message => message.Header.Offset)));
        Assert.All(cluster.Fetches, fetch => Assert.All(fetch.Request.Targets,
            target => Assert.Equal(cluster.Primary(target.PartitionId), fetch.Node)));
        Assert.Contains(cluster.Fetches, fetch => fetch.Node == 0);
        Assert.Contains(cluster.Fetches, fetch => fetch.Node == 1);
    }

    /// <summary>
    ///     The client routed partition 2 to node 0 before node 1 took it over, so the first fetch of the reader names
    ///     it at node 0, which refuses its section as not primary. The reader looks the route up again by itself.
    /// </summary>
    [Fact]
    public async Task given_a_moved_primary_when_its_section_is_refused_should_read_the_partition_on_the_new_primary()
    {
        using var cluster = new ReaderCluster(2);
        using var client = await cluster.ConnectAsync();
        await client.PollMessagesAsync(Stream, Topic, 2, Consumer.New(4), PollingStrategy.Offset(0),
            new PollOptions(), Token);
        cluster.Move(2, 1);
        await using var reader = await Reader(client, CommitPolicy.Manual, 0, 2).BuildAsync(Token);

        var delivered = await ReadAsync(reader, 2);

        Assert.Equal([(0u, 0ul), (2u, 0ul)],
            delivered.Select(batch => (batch.PartitionId, batch.Messages[0].Header.Offset)).Order());
        Assert.Contains(cluster.Fetches, fetch => fetch.Node == 0
                                                  && fetch.Request.Targets.Any(target => target.PartitionId == 2));
        Assert.Contains(cluster.Fetches, fetch => fetch.Node == 1
                                                  && fetch.Request.Targets.Any(target => target.PartitionId == 2));
    }

    [Fact]
    public async Task given_manual_commits_when_committing_should_store_a_checkpoint_of_this_reader_only()
    {
        using var cluster = new ReaderCluster(1);
        using var client = await cluster.ConnectAsync();
        await using var reader = await Reader(client, CommitPolicy.Manual, 1).BuildAsync(Token);
        var batch = await reader.NextAsync(Token);
        var polled = await client.PollMessagesAsync(Stream, Topic, 1, Consumer.New(4), PollingStrategy.Offset(0),
            new PollOptions(), Token);

        await reader.CommitAsync(batch.Checkpoint!, Token);
        var foreign = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() =>
            reader.CommitAsync(polled.Checkpoint!, Token));

        Assert.Equal(VsrError.INVALID_COMMAND, foreign.StatusCode);
        Assert.Equal(1ul, cluster.Stored[1]);
    }

    [Fact]
    public async Task given_on_read_when_committing_or_marking_should_refuse_it()
    {
        using var cluster = new ReaderCluster(1);
        using var client = await cluster.ConnectAsync();
        await using var reader = await Reader(client, CommitPolicy.OnRead, 1).BuildAsync(Token);
        var batch = await reader.NextAsync(Token);

        var commit = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() =>
            reader.CommitAsync(batch.Checkpoint!, Token));
        var mark = Assert.Throws<IggyInvalidStatusCodeException>(() => reader.Mark(batch.Checkpoint!));

        Assert.Equal((VsrError.INVALID_COMMAND, VsrError.INVALID_COMMAND), (commit.StatusCode, mark.StatusCode));
        Assert.Equal(1ul, cluster.Stored[1]);
    }

    /// <summary>
    ///     With on read commits the server stored the offsets of both partitions, but the application took only one
    ///     batch, so the close moves the other partition back before its first undelivered message.
    /// </summary>
    [Fact]
    public async Task given_on_read_when_closing_with_an_undelivered_batch_should_move_its_offset_back()
    {
        using var cluster = new ReaderCluster(1) { Limit = 2 };
        using var client = await cluster.ConnectAsync();
        var reader = await Reader(client, CommitPolicy.OnRead, 1, 2).BuildAsync(Token);
        var taken = await reader.NextAsync(Token);
        await UntilAsync(() => cluster.Stored.Count == 2);

        await reader.CloseAsync();

        var other = taken.PartitionId == 1 ? 2u : 1u;
        Assert.Equal(1ul, cluster.Stored[taken.PartitionId]);
        Assert.False(cluster.Stored.ContainsKey(other));
    }

    [Fact]
    public async Task given_on_read_when_seeking_to_an_offset_should_store_the_offset_before_it_first()
    {
        using var cluster = new ReaderCluster(1) { Limit = 10 };
        using var client = await cluster.ConnectAsync();
        var reader = await Reader(client, CommitPolicy.OnRead, 1).BuildAsync(Token);
        try
        {
            Assert.Equal([0ul, 1ul], (await reader.NextAsync(Token)).Messages.Select(message => message.Header.Offset));

            await reader.SeekAsync(1, PollingStrategy.Offset(6), Token);

            Assert.Equal(5ul, cluster.Stored[1]);
            Assert.Equal([6ul, 7ul], (await reader.NextAsync(Token)).Messages.Select(message => message.Header.Offset));
        }
        finally
        {
            await reader.CloseAsync();
        }

        Assert.Equal(7ul, cluster.Stored[1]);
    }

    [Theory]
    [InlineData(MessagePolling.Offset, 0ul, true)]
    [InlineData(MessagePolling.First, 0ul, true)]
    [InlineData(MessagePolling.Next, 0ul, true)]
    [InlineData(MessagePolling.Last, 0ul, true)]
    [InlineData(MessagePolling.Timestamp, 0ul, true)]
    [InlineData(MessagePolling.Offset, 6ul, false)]
    public async Task given_a_seek_that_moves_no_offset_when_seeking_should_store_nothing(MessagePolling kind, ulong value,
        bool onRead)
    {
        using var cluster = new ReaderCluster(1) { Limit = 10 };
        using var client = await cluster.ConnectAsync();
        var reader = await Reader(client, onRead ? CommitPolicy.OnRead : CommitPolicy.Manual, 1).BuildAsync(Token);
        try
        {
            await reader.NextAsync(Token);

            await reader.SeekAsync(1, new PollingStrategy { Kind = kind, Value = value }, Token);

            ulong? expected = onRead ? 1ul : null;
            Assert.Equal(expected, cluster.Stored.TryGetValue(1, out var stored) ? stored : null);
        }
        finally
        {
            await reader.CloseAsync();
        }
    }

    [Fact]
    public async Task given_on_read_when_the_seek_store_is_refused_should_fail_the_seek_and_move_nothing()
    {
        using var cluster = new ReaderCluster(1) { Limit = 10 };
        using var client = await cluster.ConnectAsync();
        var reader = await Reader(client, CommitPolicy.OnRead, 1, 2).BuildAsync(Token);
        try
        {
            var taken = await reader.NextAsync(Token);
            var queued = taken.PartitionId == 1 ? 2u : 1u;
            cluster.StoresRefused = true;

            var refused = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() =>
                reader.SeekAsync(queued, PollingStrategy.Offset(6), Token));

            cluster.StoresRefused = false;
            Assert.Equal(VsrError.HISTORY_UNAVAILABLE, refused.StatusCode);
            Assert.Equal(1ul, cluster.Stored[queued]);
            var next = await reader.NextAsync(Token);
            Assert.Equal(queued, next.PartitionId);
            Assert.Equal([0ul, 1ul], next.Messages.Select(message => message.Header.Offset));
            var resumed = await reader.NextAsync(Token);
            while (resumed.PartitionId != queued)
            {
                resumed = await reader.NextAsync(Token);
            }

            Assert.Equal([2ul, 3ul], resumed.Messages.Select(message => message.Header.Offset));
        }
        finally
        {
            await reader.CloseAsync();
        }
    }

    /// <summary>
    ///     The store of the seek waits for the committing read in flight, as every offset store of the partition does,
    ///     and the seek then drops the batch of that read and starts the new generation at the sought offset.
    /// </summary>
    [Fact]
    public async Task given_on_read_when_seeking_while_a_committing_read_is_in_flight_should_store_after_that_read_and_drop_it()
    {
        using var cluster = new ReaderCluster(1) { Limit = 10, HoldFetches = true };
        using var client = await cluster.ConnectAsync();
        var reader = await Reader(client, CommitPolicy.OnRead, 1).BuildAsync(Token);
        try
        {
            using var waiting = CancellationTokenSource.CreateLinkedTokenSource(Token);
            var next = reader.NextAsync(waiting.Token).AsTask();
            await UntilAsync(() => cluster.HeldFetches == 1);

            var seek = reader.SeekAsync(1, PollingStrategy.Offset(6), Token);

            await Task.Delay(TimeSpan.FromMilliseconds(200), Token);
            Assert.Equal((false, false), (cluster.Stored.ContainsKey(1), seek.IsCompleted));
            await waiting.CancelAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => next);
            cluster.HoldFetches = false;
            await cluster.ReleaseHeldAsync();
            await seek.WaitAsync(PromptLimit, Token);
            var batch = await reader.NextAsync(Token);
            Assert.Equal([6ul, 7ul], batch.Messages.Select(message => message.Header.Offset));
            Assert.Equal(2, cluster.Fetches.Count);
            var resumed = Assert.Single(cluster.Fetches.Last().Request.Targets);
            Assert.Equal((MessagePolling.Offset, 6ul), (resumed.Kind, resumed.Value));
        }
        finally
        {
            cluster.HoldFetches = false;
            await cluster.ReleaseHeldAsync();
            await reader.CloseAsync();
        }

        Assert.Equal(7ul, cluster.Stored[1]);
    }

    /// <summary>
    ///     A lost read stored offsets 2 and 3 without handing them over. The next read from Next starts at 4, so the
    ///     reader reads 2 to 5 again by offset without a commit before it goes on from Next.
    /// </summary>
    [Fact]
    public async Task given_an_oversized_first_record_when_initializing_on_read_should_keep_limits_without_a_size_retry()
    {
        var client = new Mock<IIggyClient>();
        List<PollOptions> reads = [];
        client.Setup(instance => instance.PollMessagesAsync(Stream, Topic, 1, It.IsAny<Consumer>(),
                It.IsAny<PollingStrategy>(), It.IsAny<PollOptions>(), It.IsAny<CancellationToken>()))
            .Callback<Identifier, Identifier, uint, Consumer, PollingStrategy, PollOptions, CancellationToken>(
                (_, _, _, _, _, options, _) => reads.Add(options))
            .ReturnsAsync(() => new PolledMessages
            {
                PartitionId = 1,
                CurrentOffset = 0,
                Context = ReaderCluster.Served,
                Messages = [new MessageResponse { Header = new MessageHeader { Offset = 0 }, Payload = new byte[2000] }]
            });
        var options = new PollOptions
        {
            Count = 8,
            MinCount = 3,
            MaxBytes = 512,
            MaxWait = TimeSpan.FromMilliseconds(100),
            Commit = CommitPolicy.OnRead
        };
        await using var reader = await client.Object.Reader(Stream, Topic, Consumer.New(4))
            .WithPartitions(1).WithOptions(options).BuildAsync(Token);
        var batch = await reader.NextAsync(Token);

        Assert.Equal(2000, Assert.Single(batch.Messages).Payload.Length);
        Assert.Equal(2, reads.Count);
        Assert.Equal(options with { Count = 1, MinCount = 1, Commit = CommitPolicy.Manual }, reads[0]);
        Assert.Equal(options, reads[1]);
    }

    [Theory]
    [InlineData(7ul)]
    [InlineData(9ul)]
    public async Task given_on_read_when_the_first_committing_reply_is_lost_should_recover_before_any_delivery(ulong limit)
    {
        using var cluster = new ReaderCluster(1) { Limit = limit, LoseNextCommittingRead = true };
        cluster.Stored[1] = 4;
        using var client = await cluster.ConnectAsync();
        var reader = await Reader(client, CommitPolicy.OnRead, 1).BuildAsync(Token);
        try
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(Token);
            timeout.CancelAfter(TimeSpan.FromSeconds(30));
            var batch = await reader.NextAsync(timeout.Token);
            Assert.Equal([5ul, 6ul], batch.Messages.Select(message => message.Header.Offset));
        }
        finally
        {
            await reader.CloseAsync();
        }
    }

    [Theory]
    [InlineData(0ul)]
    [InlineData(5ul)]
    public async Task given_a_lost_first_reply_when_closing_before_delivery_should_rewind_to_the_captured_start(ulong start)
    {
        using var cluster = new ReaderCluster(1) { Limit = start + 4, LoseNextCommittingRead = true };
        if (start > 0)
        {
            cluster.Stored[1] = start - 1;
        }

        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Token);
        cluster.OnLostCommittingRead = cancellation.Cancel;
        using var client = await cluster.ConnectAsync();
        var reader = await Reader(client, CommitPolicy.OnRead, 1).BuildAsync(Token);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await reader.NextAsync(cancellation.Token));
        await reader.CloseAsync();

        if (start == 0)
        {
            Assert.False(cluster.Stored.ContainsKey(1));
        }
        else
        {
            Assert.Equal(start - 1, cluster.Stored[1]);
        }
    }

    [Fact]
    public async Task given_on_read_when_a_lost_read_stored_messages_should_read_them_again_without_a_commit()
    {
        using var cluster = new ReaderCluster(1) { Limit = 8 };
        using var client = await cluster.ConnectAsync();
        await using var reader = await Reader(client, CommitPolicy.OnRead, 1).BuildAsync(Token);
        Assert.Equal([0ul, 1ul], (await reader.NextAsync(Token)).Messages.Select(message => message.Header.Offset));
        await UntilAsync(() => cluster.Stored.TryGetValue(1, out var stored) && stored == 1);

        cluster.LoseNextCommittingRead = true;
        var delivered = await ReadAsync(reader, 3, LostReplyLimit);

        Assert.Equal([2ul, 3ul, 4ul, 5ul, 6ul, 7ul],
            delivered.SelectMany(batch => batch.Messages).Select(message => message.Header.Offset));
        Assert.Equal([(MessagePolling.Offset, 2ul, false), (MessagePolling.Offset, 4ul, false)],
            cluster.Polls.Where(poll => poll.Kind == MessagePolling.Offset)
                .Select(poll => (poll.Kind, poll.Value, poll.AutoCommit)));
        Assert.Equal(7ul, cluster.Stored[1]);
    }

    /// <summary>
    ///     A lost read stored offsets 4 and 5, so the next read from Next skipped to 6 and the server stored up to 9.
    ///     The reads again by offset stop at 9, so the next read from Next starts at 10 and delivers nothing twice.
    /// </summary>
    [Fact]
    public async Task given_on_read_when_a_lost_read_is_read_again_should_stop_at_the_stored_end()
    {
        var log = new ScriptedLog { End = 4 };
        await using var reader = await log.Client().Reader(Stream, Topic, Consumer.New(4)).WithPartitions(1)
            .WithOptions(new PollOptions
            {
                Count = 4,
                MaxWait = TimeSpan.FromMilliseconds(100),
                Commit = CommitPolicy.OnRead
            })
            .WithRetryInterval(RetryInterval).BuildAsync(Token);
        var first = await reader.NextAsync(Token);
        log.End = 6;
        log.LoseNextCommittingRead = true;

        var delivered = await ReadAsync(reader, 3);

        Assert.Equal(Enumerable.Range(0, 14).Select(offset => (ulong)offset),
            delivered.Prepend(first).SelectMany(batch => batch.Messages).Select(message => message.Header.Offset));
    }

    /// <summary>
    ///     The committing read of offsets 4 to 7 stored its offset, but its reply does not decode. The reader reads
    ///     the range again by offset one message at a time, reports the bad message 5 with a skip past it, and after
    ///     the seek delivers the rest.
    /// </summary>
    [Fact]
    public async Task given_on_read_when_a_committing_poll_does_not_decode_should_read_its_range_again_and_skip_only_the_bad_message()
    {
        var log = new ScriptedLog { End = 8, Malformed = { 5 } };
        await using var reader = await log.Client().Reader(Stream, Topic, Consumer.New(4)).WithPartitions(1)
            .WithOptions(new PollOptions
            {
                Count = 4,
                MaxWait = TimeSpan.FromMilliseconds(100),
                Commit = CommitPolicy.OnRead
            })
            .WithRetryInterval(RetryInterval).BuildAsync(Token);
        var first = await reader.NextAsync(Token);
        var second = await reader.NextAsync(Token);

        var error = await NextErrorAsync(reader);
        await reader.SeekAsync(1, error.Skip!.Value, Token);
        var third = await reader.NextAsync(Token);

        Assert.Equal([0ul, 1ul, 2ul, 3ul, 4ul, 6ul, 7ul],
            new[] { first, second, third }.SelectMany(batch => batch.Messages).Select(message => message.Header.Offset));
        Assert.Equal((MessagePolling.Offset, 6ul), (error.Skip.Value.Kind, error.Skip.Value.Value));
    }

    /// <summary>
    ///     A lost read stored offsets 4 and 5, which the next read skipped, and every read of them again comes back
    ///     empty. A read again asks for no wait, so the reader waits its retry interval between them.
    /// </summary>
    [Fact]
    public async Task given_on_read_when_reading_a_lost_range_again_finds_nothing_should_wait_its_retry_interval()
    {
        var log = new ScriptedLog { End = 4, RecoveryEmpty = true };
        var reader = await log.Client().Reader(Stream, Topic, Consumer.New(4)).WithPartitions(1)
            .WithOptions(new PollOptions
            {
                Count = 4,
                MaxWait = TimeSpan.FromMilliseconds(100),
                Commit = CommitPolicy.OnRead
            })
            .WithRetryInterval(RetryInterval).BuildAsync(Token);
        await reader.NextAsync(Token);
        log.End = 6;
        log.LoseNextCommittingRead = true;
        var window = TimeSpan.FromMilliseconds(500);
        using (var waiting = CancellationTokenSource.CreateLinkedTokenSource(Token))
        {
            waiting.CancelAfter(window);
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await reader.NextAsync(waiting.Token));
        }

        await reader.DisposeAsync();

        Assert.InRange(log.RecoveryReads, 1, (int)(window / RetryInterval) + 2);
    }

    /// <summary>
    ///     With on read commits a seek waits for the read of its partition in flight. The reader closes meanwhile, so
    ///     no read takes the new position and the close does not store it. The seek fails instead of reporting a
    ///     position that nothing applies.
    /// </summary>
    /// <summary>
    ///     The seek's store waits for the read in flight, and the close waits for the seek before its rewind, so the
    ///     rewind sees the sought position and keeps that offset instead of moving back before the dropped batch.
    /// </summary>
    [Fact]
    public async Task given_on_read_when_the_reader_closes_while_a_seek_waits_for_its_read_should_apply_the_seek_and_keep_its_offset()
    {
        using var cluster = new ReaderCluster(1) { Limit = 10, HoldFetches = true };
        using var client = await cluster.ConnectAsync();
        var reader = await Reader(client, CommitPolicy.OnRead, 1).BuildAsync(Token);
        var next = reader.NextAsync(Token).AsTask();
        await UntilAsync(() => cluster.HeldFetches == 1);
        var seek = reader.SeekAsync(1, PollingStrategy.Offset(6), Token);
        await Task.Delay(TimeSpan.FromMilliseconds(200), Token);

        var closing = reader.CloseAsync().AsTask();
        cluster.HoldFetches = false;
        await cluster.ReleaseHeldAsync();
        await closing.WaitAsync(PromptLimit, Token);

        await seek.WaitAsync(PromptLimit, Token);
        await Assert.ThrowsAnyAsync<Exception>(() => next);
        Assert.Equal(5ul, cluster.Stored[1]);
    }

    /// <summary>
    ///     A committing fetch waits on the server for 30 seconds. The close cancels it on the server and ends once the
    ///     server answers the cancel, instead of waiting out the fetch.
    /// </summary>
    [Fact]
    public async Task given_on_read_when_closing_with_a_parked_committing_fetch_should_cancel_it_and_end_at_its_answer()
    {
        using var cluster = new ReaderCluster(1) { Limit = 0 };
        using var client = await cluster.ConnectAsync();
        var reader = await client.Reader(Stream, Topic, Consumer.New(4))
            .WithPartitions(1)
            .WithStart(PollingStrategy.Offset(0))
            .WithOptions(new PollOptions { MaxWait = TimeSpan.FromSeconds(30), Commit = CommitPolicy.OnRead })
            .WithRetryInterval(RetryInterval).BuildAsync(Token);
        using var waiting = CancellationTokenSource.CreateLinkedTokenSource(Token);
        var next = reader.NextAsync(waiting.Token).AsTask();
        await UntilAsync(() => cluster.Fetches.Any(fetch => fetch.Request.AutoCommit));

        await reader.CloseAsync().AsTask().WaitAsync(PromptLimit, Token);
        await waiting.CancelAsync();

        Assert.Equal(1, cluster.Cancels);
        await Assert.ThrowsAnyAsync<Exception>(() => next);
    }

    /// <summary>
    ///     The first close waits for a committing read whose cancel the server has not answered yet. A second close
    ///     waits for the first instead of returning while it still runs.
    /// </summary>
    [Fact]
    public async Task given_a_close_in_progress_when_closing_again_should_wait_for_the_first_close()
    {
        using var cluster = new ReaderCluster(1) { Limit = 4, HoldFetches = true };
        using var client = await cluster.ConnectAsync();
        var reader = await Reader(client, CommitPolicy.OnRead, 1).WithStart(PollingStrategy.Offset(0)).BuildAsync(Token);
        using var waiting = CancellationTokenSource.CreateLinkedTokenSource(Token);
        var next = reader.NextAsync(waiting.Token).AsTask();
        await UntilAsync(() => cluster.HeldFetches == 1);
        var first = reader.CloseAsync().AsTask();

        var second = reader.CloseAsync().AsTask();
        await Task.WhenAny(second, Task.Delay(TimeSpan.FromMilliseconds(200), Token));
        var waited = !second.IsCompleted;
        cluster.HoldFetches = false;
        await cluster.ReleaseHeldAsync();
        await first.WaitAsync(PromptLimit, Token);
        await second.WaitAsync(PromptLimit, Token);
        await waiting.CancelAsync();

        Assert.True(waited);
        await Assert.ThrowsAnyAsync<Exception>(() => next);
    }

    /// <summary>
    ///     Another session stored offset 3 while no read of this reader was lost, so the next read from Next starts at
    ///     4. The reader delivers from there and reads nothing again.
    /// </summary>
    [Fact]
    public async Task given_on_read_when_the_stored_offset_moves_ahead_without_a_lost_read_should_deliver_from_there()
    {
        using var cluster = new ReaderCluster(1) { Limit = 8 };
        using var client = await cluster.ConnectAsync();
        await using var reader = await Reader(client, CommitPolicy.OnRead, 1).BuildAsync(Token);
        Assert.Equal([0ul, 1ul], (await reader.NextAsync(Token)).Messages.Select(message => message.Header.Offset));
        await UntilAsync(() => cluster.Stored.TryGetValue(1, out var stored) && stored == 1);

        cluster.Stored[1] = 3;
        var delivered = await ReadAsync(reader, 2);

        Assert.Equal([4ul, 5ul, 6ul, 7ul],
            delivered.SelectMany(batch => batch.Messages).Select(message => message.Header.Offset));
        Assert.DoesNotContain(cluster.Polls, poll => poll.Kind == MessagePolling.Offset);
    }

    /// <summary>
    ///     Every committing fetch of partition 1 ends with an unknown outcome. The reader abandons each one and waits
    ///     its retry interval before it reads the partition again, instead of fetching at the pace of the round trip.
    /// </summary>
    [Fact]
    public async Task given_on_read_when_every_committing_read_ends_uncertain_should_wait_its_retry_interval_after_each()
    {
        using var cluster = new ReaderCluster(1) { Limit = 1000 };
        cluster.UncertainSections.Add(1);
        using var client = await cluster.ConnectAsync();
        var reader = await Reader(client, CommitPolicy.OnRead, 1).BuildAsync(Token);
        var window = TimeSpan.FromMilliseconds(500);
        using (var waiting = CancellationTokenSource.CreateLinkedTokenSource(Token))
        {
            waiting.CancelAfter(window);
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await reader.NextAsync(waiting.Token));
        }

        await reader.DisposeAsync();

        Assert.InRange(cluster.Fetches.Count(fetch => fetch.Request.AutoCommit), 1, (int)(window / RetryInterval) + 2);
    }

    /// <summary>
    ///     A lost read stored offsets 5 and 6, the next read from Next served 7 and 8, and the reader closes while it
    ///     reads 5 and 6 again. Nothing was delivered, so the close moves the offset back before the lost read.
    /// </summary>
    [Fact]
    public async Task given_on_read_when_closing_while_a_lost_read_is_read_again_should_move_the_offset_back_before_it()
    {
        using var cluster = new ReaderCluster(1) { Limit = 9, LoseNextCommittingRead = true, RecoveryUnanswered = true };
        cluster.Stored[1] = 4;
        using var client = await cluster.ConnectAsync();
        var reader = await Reader(client, CommitPolicy.OnRead, 1).BuildAsync(Token);
        using var waiting = CancellationTokenSource.CreateLinkedTokenSource(Token);
        var next = reader.NextAsync(waiting.Token).AsTask();
        await UntilAsync(() => cluster.Polls.Any(poll => poll is { Kind: MessagePolling.Offset, Value: 5, AutoCommit: false }),
            (long)LostReplyLimit.TotalMilliseconds);
        await waiting.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => next);

        await reader.CloseAsync();

        Assert.Equal(4ul, cluster.Stored[1]);
    }

    /// <summary>
    ///     A rejoin gave the partition a new owner generation, so the server refuses a read from the old position as
    ///     not owned. The reader drops that position and reads on from the stored offset.
    /// </summary>
    [Fact]
    public async Task given_a_new_owner_generation_when_a_read_is_refused_as_not_owned_should_read_on_from_the_stored_offset()
    {
        using var cluster = new ReaderCluster(1) { Assignment = [1] };
        using var client = await cluster.ConnectAsync();
        var reader = await client.Reader(Stream, Topic, Consumer.Group("readers"))
            .WithOptions(new PollOptions { MaxWait = TimeSpan.FromMilliseconds(100) })
            .WithRetryInterval(RetryInterval).BuildAsync(Token);
        try
        {
            await reader.CommitAsync((await reader.NextAsync(Token)).Checkpoint!, Token);
            cluster.OwnerGeneration = ReaderCluster.Served.OwnerGeneration + 1;
            cluster.Limit = 4;

            var next = (await ReadAsync(reader, 1))[0];

            Assert.Equal([2ul, 3ul], next.Messages.Select(message => message.Header.Offset));
            Assert.Equal(cluster.OwnerGeneration, next.Context.OwnerGeneration);
        }
        finally
        {
            await reader.CloseAsync();
        }
    }

    [Fact]
    public async Task given_a_partition_that_stays_not_owned_when_reading_should_report_it_after_lasting_refusals()
    {
        using var cluster = new ReaderCluster(1) { Assignment = [1] };
        cluster.NotOwned.Add(1);
        using var client = await cluster.ConnectAsync();
        var reader = await client.Reader(Stream, Topic, Consumer.Group("readers"))
            .WithOptions(new PollOptions { MaxWait = TimeSpan.FromMilliseconds(100) })
            .WithRetryInterval(RetryInterval).BuildAsync(Token);
        try
        {
            var error = await NextErrorAsync(reader);

            Assert.Equal((1u, VsrError.CONSUMER_GROUP_PARTITION_NOT_OWNED),
                (error.PartitionId, Assert.IsType<IggyInvalidStatusCodeException>(error.InnerException).StatusCode));
        }
        finally
        {
            await reader.CloseAsync();
        }
    }

    /// <summary>
    ///     The read of partition 1 is still in flight when a rebalance takes the partition away and gives it back, and
    ///     the new assignment starts its own read. The old read ends first, which must not free the new one, or a
    ///     second read of the same position starts and its batch is delivered twice.
    /// </summary>
    [Fact]
    public async Task given_a_partition_assigned_again_when_its_old_read_ends_should_not_read_the_new_position_twice()
    {
        using var cluster = new ReaderCluster(1) { Limit = 4, Assignment = [1], HoldFetches = true };
        using var client = await cluster.ConnectAsync();
        var reader = await client.Reader(Stream, Topic, Consumer.Group("readers"))
            .WithStart(PollingStrategy.Offset(0))
            .WithOptions(new PollOptions { MaxWait = TimeSpan.FromSeconds(30) })
            .WithRetryInterval(RetryInterval).BuildAsync(Token);
        try
        {
            await UntilAsync(() => cluster.HeldFetches == 1);
            cluster.Assignment = [];
            await UntilAsync(() => reader.Partitions.Count == 0, ConsumerGroupClientState.AssignmentRefreshMs * 2);
            cluster.Assignment = [1];
            await UntilAsync(() => reader.Partitions.Count == 1 && cluster.HeldFetches == 2,
                ConsumerGroupClientState.AssignmentRefreshMs * 2);

            await cluster.ReleaseHeldAsync(1);
            // A second read of the new assignment would start now.
            await Task.Delay(TimeSpan.FromMilliseconds(200), Token);
            cluster.HoldFetches = false;
            await cluster.ReleaseHeldAsync();
            var delivered = await ReadAsync(reader, 2);

            Assert.Equal([0ul, 1ul, 2ul, 3ul],
                delivered.SelectMany(batch => batch.Messages).Select(message => message.Header.Offset));
        }
        finally
        {
            await reader.CloseAsync();
        }
    }

    [Fact]
    public async Task given_on_read_when_reassignment_interrupts_gap_recovery_should_report_the_gap_and_deliver_the_stored_batch()
    {
        using var cluster = new ReaderCluster(1) { Limit = 6, Assignment = [1] };
        using var client = await cluster.ConnectAsync();
        var reader = await client.Reader(Stream, Topic, Consumer.Group("readers"))
            .WithOptions(new PollOptions { MaxWait = TimeSpan.FromMilliseconds(100), Commit = CommitPolicy.OnRead })
            .WithRetryInterval(RetryInterval).BuildAsync(Token);
        try
        {
            Assert.Equal([0ul, 1ul], (await reader.NextAsync(Token)).Messages.Select(message => message.Header.Offset));
            cluster.LoseNextCommittingRead = true;
            cluster.AssignmentAfterRead = [];
            cluster.RecoveryNotOwned = true;
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(Token);
            timeout.CancelAfter(LostReplyLimit);
            var error = await Assert.ThrowsAsync<ReadException>(async () => await reader.NextAsync(timeout.Token));
            Assert.Equal(VsrError.FETCH_PROGRESS_UNCERTAIN,
                Assert.IsAssignableFrom<IggyInvalidStatusCodeException>(error.InnerException).StatusCode);
            Assert.Equal([4ul, 5ul], (await reader.NextAsync(timeout.Token)).Messages.Select(message => message.Header.Offset));
        }
        finally
        {
            await reader.CloseAsync();
        }
    }

    [Fact]
    public async Task given_a_message_that_does_not_decode_when_reading_should_report_a_skip_and_resume_after_a_seek()
    {
        using var cluster = new ReaderCluster(1) { Limit = 5 };
        cluster.Undecodable.Add(2);
        using var client = await cluster.ConnectAsync();
        await using var reader = await Reader(client, CommitPolicy.Manual, 1).BuildAsync(Token);
        Assert.Equal(0ul, (await reader.NextAsync(Token)).Messages[0].Header.Offset);

        var error = await Assert.ThrowsAsync<ReadException>(() => reader.NextAsync(Token).AsTask());
        await reader.SeekAsync(1, error.Skip!.Value, Token);
        var resumed = await reader.NextAsync(Token);

        Assert.Equal((1u, MessagePolling.Offset, 3ul), (error.PartitionId, error.Skip.Value.Kind, error.Skip.Value.Value));
        Assert.IsType<MalformedResponseException>(error.InnerException);
        Assert.Null(error.Offset);
        Assert.Equal(GoldenPollBody(1, 2, ReaderCluster.Served)[DeferredPollOptions.POLL_REPLY_HEADER_SIZE..^1],
            error.Record);
        Assert.Equal([3ul, 4ul], resumed.Messages.Select(message => message.Header.Offset));
    }

    [Fact]
    public async Task given_a_message_that_does_not_decrypt_when_reading_should_report_its_offset_and_frame_as_served()
    {
        using var cluster = new ReaderCluster(1) { Limit = 5 };
        using var encryptor = new AesMessageEncryptor(AesMessageEncryptor.GenerateKey());
        using var client = await cluster.ConnectAsync(encryptor);
        await using var reader = await Reader(client, CommitPolicy.Manual, 1).BuildAsync(Token);
        var frameStart = DeferredPollOptions.POLL_REPLY_HEADER_SIZE + BatchWireFormat.BATCH_HEADER_SIZE;
        var frameEnd = frameStart + BatchWireFormat.FRAME_HEADER_SIZE + "first-payload"u8.Length;

        var error = await Assert.ThrowsAsync<ReadException>(() => reader.NextAsync(Token).AsTask());

        Assert.IsType<MessageDecryptionException>(error.InnerException);
        Assert.Equal((1u, 0ul, MessagePolling.Offset, 1ul),
            (error.PartitionId, error.Offset, error.Skip!.Value.Kind, error.Skip.Value.Value));
        Assert.Equal(GoldenPollBody(1, 0, ReaderCluster.Served)[frameStart..frameEnd], error.Record);
    }

    [Fact]
    public async Task given_a_lost_history_on_one_partition_when_reading_should_stop_only_it_until_a_seek()
    {
        using var cluster = new ReaderCluster(1) { Limit = 1000 };
        using var client = await cluster.ConnectAsync();
        await using var reader = await Reader(client, CommitPolicy.Manual, 1, 2).BuildAsync(Token);
        await ReadAsync(reader, 2);
        cluster.HistoryRefused.Add(1);

        var error = await NextErrorAsync(reader);
        var others = await ReadAsync(reader, 4);
        await reader.SeekAsync(1, PollingStrategy.Offset(0), Token);
        cluster.HistoryRefused.Clear();
        var resumed = await NextOfAsync(reader, 1);

        Assert.Equal((1u, VsrError.HISTORY_UNAVAILABLE),
            (error.PartitionId, Assert.IsType<IggyInvalidStatusCodeException>(error.InnerException).StatusCode));
        Assert.All(others, batch => Assert.Equal(2u, batch.PartitionId));
        Assert.Equal((1u, 0ul), (resumed.PartitionId, resumed.Messages[0].Header.Offset));
    }

    [Fact]
    public async Task given_a_last_start_with_an_oversized_first_message_when_reading_should_stop_the_partition_until_a_seek()
    {
        using var cluster = new ReaderCluster(1) { Limit = 4, OversizedLast = true };
        using var client = await cluster.ConnectAsync();
        await using var reader = await Reader(client, CommitPolicy.Manual, 1)
            .WithStart(PollingStrategy.Last()).BuildAsync(Token);

        var error = await Assert.ThrowsAsync<ReadException>(() => reader.NextAsync(Token).AsTask());
        await reader.SeekAsync(1, PollingStrategy.Offset(2), Token);
        var resumed = await reader.NextAsync(Token);

        Assert.Equal(VsrError.INVALID_SIZE_BYTES,
            Assert.IsType<IggyInvalidStatusCodeException>(error.InnerException).StatusCode);
        Assert.Equal(2ul, resumed.Messages[0].Header.Offset);
    }

    [Fact]
    public async Task given_a_backward_seek_when_reading_should_deliver_the_earlier_messages_again()
    {
        using var cluster = new ReaderCluster(1) { Limit = 4 };
        using var client = await cluster.ConnectAsync();
        await using var reader = await Reader(client, CommitPolicy.Manual, 1).BuildAsync(Token);
        await ReadAsync(reader, 2);

        await reader.SeekAsync(1, PollingStrategy.Offset(0), Token);
        var again = await reader.NextAsync(Token);

        Assert.Equal([0ul, 1ul], again.Messages.Select(message => message.Header.Offset));
    }

    [Theory]
    [InlineData(CommitAfter.Delivery)]
    [InlineData(CommitAfter.Processing)]
    public async Task given_auto_commit_when_closing_should_store_the_last_counted_position(CommitAfter after)
    {
        using var cluster = new ReaderCluster(1) { Limit = 4 };
        using var client = await cluster.ConnectAsync();
        var policy = CommitPolicy.Auto(new AutoCommit { Interval = TimeSpan.FromHours(1), After = after });
        var reader = await Reader(client, policy, 1).BuildAsync(Token);
        await ReadAsync(reader, 2);

        await reader.CloseAsync();

        Assert.Equal(3ul, cluster.Stored[1]);
    }

    [Fact]
    public async Task given_auto_commit_after_processing_each_batch_when_asking_for_the_next_should_store_the_previous()
    {
        using var cluster = new ReaderCluster(1) { Limit = 6 };
        using var client = await cluster.ConnectAsync();
        var policy = CommitPolicy.Auto(new AutoCommit { Every = CommitEvery.Batch, After = CommitAfter.Processing });
        await using var reader = await Reader(client, policy, 1).BuildAsync(Token);
        await reader.NextAsync(Token);
        await Task.Delay(TimeSpan.FromMilliseconds(200), Token);
        Assert.False(cluster.Stored.ContainsKey(1));

        await reader.NextAsync(Token);

        await UntilAsync(() => cluster.Stored.TryGetValue(1, out var stored) && stored == 1);
    }

    [Fact]
    public async Task given_auto_commit_after_marks_when_marking_should_store_only_the_marked_position()
    {
        using var cluster = new ReaderCluster(1) { Limit = 4 };
        using var client = await cluster.ConnectAsync();
        var policy = CommitPolicy.Auto(new AutoCommit { Interval = TimeSpan.FromHours(1), After = CommitAfter.Marked });
        var reader = await Reader(client, policy, 1).BuildAsync(Token);
        var batches = await ReadAsync(reader, 2);

        reader.Mark(batches[0].CheckpointAt(0)!);
        await reader.CloseAsync();

        Assert.Equal(0ul, cluster.Stored[1]);
    }

    [Fact]
    public async Task given_a_group_reader_when_built_should_join_and_read_the_assignment_of_its_member()
    {
        using var cluster = new ReaderCluster(2) { Assignment = [1, 2] };
        using var client = await cluster.ConnectAsync();
        var reader = await client.Reader(Stream, Topic, Consumer.Group("readers"))
            .WithOptions(new PollOptions { MaxWait = TimeSpan.FromMilliseconds(100) })
            .WithRetryInterval(RetryInterval).BuildAsync(Token);

        var delivered = await ReadAsync(reader, 2);
        await reader.CloseAsync();

        Assert.Equal([1u, 2u], delivered.Select(batch => batch.PartitionId).Order());
        Assert.Equal([1u, 2u], cluster.Fetches.SelectMany(fetch => fetch.Request.Targets)
            .Select(target => target.PartitionId).Distinct().Order());
        Assert.Equal((1, 1), (cluster.Joins, cluster.Leaves));
    }

    /// <summary>
    ///     The coordinator hands a joining member its partitions one at a time, so the first assignment holds only
    ///     partition 1. The reader asks again at its refresh interval and reads partition 3 well before the client
    ///     cache of the assignment expires.
    /// </summary>
    [Fact]
    public async Task given_a_group_reader_when_its_assignment_grows_should_read_the_new_partition_within_its_refresh()
    {
        using var cluster = new ReaderCluster(2) { Assignment = [1] };
        using var client = await cluster.ConnectAsync();
        var reader = await client.Reader(Stream, Topic, Consumer.Group("readers"))
            .WithOptions(new PollOptions { MaxWait = TimeSpan.FromMilliseconds(100) })
            .WithRetryInterval(RetryInterval).BuildAsync(Token);
        try
        {
            Assert.Equal(1u, (await reader.NextAsync(Token)).PartitionId);
            cluster.Assignment = [1, 3];

            var added = await NextOfAsync(reader, 3, TimeSpan.FromMilliseconds(2500));

            Assert.Equal([0ul, 1ul], added.Messages.Select(message => message.Header.Offset));
        }
        finally
        {
            await reader.CloseAsync();
        }
    }

    /// <summary>
    ///     The server stored the on read fetch of both partitions, then partition 3 left the assignment of the member
    ///     before the application took its batch. Dropping the batch would lose its messages.
    /// </summary>
    [Fact]
    public async Task given_on_read_when_a_partition_leaves_with_an_undelivered_batch_should_still_hand_it_over()
    {
        using var cluster = new ReaderCluster(2) { Assignment = [1, 3], AssignmentAfterRead = [1] };
        using var client = await cluster.ConnectAsync();
        var reader = await client.Reader(Stream, Topic, Consumer.Group("readers"))
            .WithStart(PollingStrategy.Offset(0))
            .WithOptions(new PollOptions { MaxWait = TimeSpan.FromMilliseconds(100), Commit = CommitPolicy.OnRead })
            .WithRetryInterval(RetryInterval).BuildAsync(Token);
        var first = await reader.NextAsync(Token);
        var synced = cluster.Syncs;
        await UntilAsync(() => cluster.Syncs > synced, ConsumerGroupClientState.AssignmentRefreshMs * 2);
        // The reader applies the reply of the sync after the node counted it.
        await Task.Delay(100, Token);

        var second = await ReadAsync(reader, 1);
        await reader.CloseAsync();

        Assert.Equal([1u, 3u], new[] { first.PartitionId, second[0].PartitionId }.Order());
        Assert.Equal([0ul, 1ul], second[0].Messages.Select(message => message.Header.Offset));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task given_on_read_when_a_partition_returns_should_keep_its_stored_batch_unless_sought(bool seek)
    {
        using var cluster = new ReaderCluster(2) { Limit = 4, Assignment = [1, 3], AssignmentAfterRead = [1] };
        using var client = await cluster.ConnectAsync();
        var reader = await client.Reader(Stream, Topic, Consumer.Group("readers"))
            .WithStart(PollingStrategy.Offset(0))
            .WithOptions(new PollOptions { MaxWait = TimeSpan.FromMilliseconds(100), Commit = CommitPolicy.OnRead })
            .WithRetryInterval(RetryInterval).BuildAsync(Token);
        try
        {
            Assert.Equal(1u, (await reader.NextAsync(Token)).PartitionId);
            await UntilAsync(() => !reader.Partitions.Contains(3u), ConsumerGroupClientState.AssignmentRefreshMs * 2);
            cluster.Assignment = [1, 3];
            await UntilAsync(() => reader.Partitions.Contains(3u), ConsumerGroupClientState.AssignmentRefreshMs * 2);
            if (seek)
            {
                await reader.SeekAsync(3, PollingStrategy.Offset(2), Token);
            }

            var batch = (await ReadAsync(reader, 1))[0];
            while (batch.PartitionId != 3)
            {
                batch = (await ReadAsync(reader, 1))[0];
            }

            Assert.Equal(seek ? [2ul, 3ul] : [0ul, 1ul], batch.Messages.Select(message => message.Header.Offset));
        }
        finally
        {
            await reader.CloseAsync();
        }
    }

    [Fact]
    public async Task given_a_failed_route_lookup_when_the_caller_asks_again_at_once_should_wait_its_retry_interval()
    {
        using var cluster = new ReaderCluster(1) { RoutesRefused = true };
        using var client = await cluster.ConnectAsync();
        var reader = await Reader(client, CommitPolicy.OnRead, 1).WithStart(PollingStrategy.Offset(0)).BuildAsync(Token);
        var window = TimeSpan.FromMilliseconds(500);
        var until = DateTime.UtcNow + window;
        var readerErrors = 0;

        while (DateTime.UtcNow < until)
        {
            using var attempt = CancellationTokenSource.CreateLinkedTokenSource(Token);
            attempt.CancelAfter(TimeSpan.FromMilliseconds(50));
            try
            {
                await reader.NextAsync(attempt.Token);
            }
            catch (ReadException)
            {
                readerErrors++;
            }
            catch (OperationCanceledException) when (!Token.IsCancellationRequested)
            {
            }
        }

        await reader.CloseAsync();

        Assert.True(readerErrors > 0);
        Assert.InRange(cluster.RouteLookups, 1, (int)(window / RetryInterval) + 2);
    }

    [Fact]
    public async Task given_partitions_of_one_primary_when_a_fetch_reaches_its_count_should_give_each_partition_its_turn()
    {
        using var cluster = new ReaderCluster(2) { Limit = 100, CountLimited = true };
        using var client = await cluster.ConnectAsync();
        var reader = await client.Reader(Stream, Topic, Consumer.New(4))
            .WithPartitions(1, 3, 5)
            .WithOptions(new PollOptions { Count = 2, MaxWait = TimeSpan.FromMilliseconds(100) })
            .WithRetryInterval(RetryInterval).BuildAsync(Token);

        var delivered = await ReadAsync(reader, 3);
        await reader.CloseAsync();

        Assert.Equal([1u, 3u, 5u], delivered.Select(batch => batch.PartitionId));
    }

    /// <summary>
    ///     Every partition of the primary ended its last read before its head, so each one reads in a fetch of its own,
    ///     with the full limits, and the reader keeps as many reads in flight as separate polls would.
    /// </summary>
    [Fact]
    public async Task given_backlogged_partitions_of_one_primary_when_slots_are_free_should_read_each_in_its_own_fetch()
    {
        using var cluster = new ReaderCluster(1) { Limit = 100, ReportsHead = true, HoldFetches = true };
        using var client = await cluster.ConnectAsync();
        var reader = await Reader(client, CommitPolicy.Manual, 0, 1, 2, 3).BuildAsync(Token);
        try
        {
            await UntilAsync(() => cluster.HeldFetches == 1);
            await cluster.ReleaseHeldAsync();

            await UntilAsync(() => cluster.Held.Sum(fetch => fetch.Targets.Length) == 4);

            Assert.All(cluster.Held, fetch => Assert.Single(fetch.Targets));
            Assert.Equal(4, cluster.HeldFetches);
        }
        finally
        {
            cluster.HoldFetches = false;
            await cluster.ReleaseHeldAsync();
            await reader.CloseAsync();
        }
    }

    /// <summary>
    ///     A committing reader reads only while <c>NextAsync</c> waits, and then deals its backlogged partitions over the
    ///     free slots as a reader that commits nothing does, one committing fetch per partition.
    /// </summary>
    [Fact]
    public async Task given_backlogged_partitions_of_one_primary_when_reading_on_read_should_commit_each_in_its_own_fetch()
    {
        using var cluster = new ReaderCluster(1) { Limit = 100, ReportsHead = true, HoldFetches = true };
        using var client = await cluster.ConnectAsync();
        // A start by offset needs no check poll, so the four partitions share the first fetch.
        var reader = await Reader(client, CommitPolicy.OnRead, 0, 1, 2, 3).WithStart(PollingStrategy.Offset(0))
            .BuildAsync(Token);
        try
        {
            var first = reader.NextAsync(Token).AsTask();
            await UntilAsync(() => cluster.HeldFetches == 1);
            await cluster.ReleaseHeldAsync();
            List<PolledMessages> delivered = [await first.WaitAsync(PromptLimit, Token)];
            delivered.AddRange(await ReadAsync(reader, 3));
            Assert.Equal([0u, 1u, 2u, 3u], delivered.Select(batch => batch.PartitionId).Order());

            var fifth = reader.NextAsync(Token).AsTask();

            await UntilAsync(() => cluster.Held.Sum(fetch => fetch.Targets.Length) == 4);
            Assert.All(cluster.Held, fetch => Assert.Single(fetch.Targets));
            Assert.All(cluster.Held, fetch => Assert.True(fetch.AutoCommit));
            Assert.Equal(4, cluster.HeldFetches);
            cluster.HoldFetches = false;
            await cluster.ReleaseHeldAsync();
            Assert.Equal([2ul, 3ul],
                (await fifth.WaitAsync(PromptLimit, Token)).Messages.Select(message => message.Header.Offset));
        }
        finally
        {
            cluster.HoldFetches = false;
            await cluster.ReleaseHeldAsync();
            await reader.CloseAsync();
        }
    }

    [Fact]
    public async Task given_reads_that_do_not_wait_when_a_reply_leaves_a_target_out_should_wait_its_retry_interval()
    {
        using var cluster = new ReaderCluster(1) { Sectionless = true };
        using var client = await cluster.ConnectAsync();
        var reader = await client.Reader(Stream, Topic, Consumer.New(4))
            .WithPartitions(1)
            .WithOptions(new PollOptions { MaxWait = TimeSpan.Zero })
            .WithRetryInterval(RetryInterval).BuildAsync(Token);
        var window = TimeSpan.FromMilliseconds(500);

        await Task.Delay(window, Token);
        await reader.CloseAsync();

        Assert.InRange(cluster.Fetches.Count, 1, (int)(window / RetryInterval) + 2);
    }

    [Fact]
    public async Task given_invalid_configurations_when_building_should_refuse_them()
    {
        using var cluster = new ReaderCluster(1);
        using var client = await cluster.ConnectAsync();
        var polled = await client.PollMessagesAsync(Stream, Topic, 1, Consumer.New(4), PollingStrategy.Offset(0),
            new PollOptions(), Token);

        var noPartitions = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() =>
            client.Reader(Stream, Topic, Consumer.New(4)).BuildAsync(Token));
        var groupPartitions = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() =>
            client.Reader(Stream, Topic, Consumer.Group("readers")).WithPartitions(1).BuildAsync(Token));
        var continuation = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() =>
            Reader(client, CommitPolicy.Manual, 1).WithStart(polled.Continuation).BuildAsync(Token));
        var groupOverHttp = await Assert.ThrowsAsync<FeatureUnavailableException>(() =>
            new HttpMessageStream(new HttpClient { BaseAddress = new Uri("http://localhost") })
                .Reader(Stream, Topic, Consumer.Group("readers")).BuildAsync(Token));

        Assert.Equal((VsrError.INVALID_COMMAND, VsrError.INVALID_COMMAND, VsrError.INVALID_COMMAND),
            (noPartitions.StatusCode, groupPartitions.StatusCode, continuation.StatusCode));
        Assert.NotNull(groupOverHttp);
        Assert.Equal(0, cluster.Joins);
    }

    private static CancellationToken Token => TestContext.Current.CancellationToken;

    private static ReaderBuilder Reader(TcpMessageStream client, CommitPolicy commit, params uint[] partitions)
    {
        return client.Reader(Stream, Topic, Consumer.New(4))
            .WithPartitions(partitions)
            .WithOptions(new PollOptions { MaxWait = TimeSpan.FromMilliseconds(100), Commit = commit })
            .WithRetryInterval(RetryInterval);
    }

    private static async Task<List<PolledMessages>> ReadAsync(PartitionReader reader, int batches,
        TimeSpan? limit = null)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(Token);
        timeout.CancelAfter(limit ?? PromptLimit);
        List<PolledMessages> delivered = [];
        while (delivered.Count < batches)
        {
            delivered.Add(await reader.NextAsync(timeout.Token));
        }

        return delivered;
    }

    private static async Task<PolledMessages> NextOfAsync(PartitionReader reader, uint partition,
        TimeSpan? limit = null)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(Token);
        timeout.CancelAfter(limit ?? PromptLimit);
        while (true)
        {
            var batch = await reader.NextAsync(timeout.Token);
            if (batch.PartitionId == partition)
            {
                return batch;
            }
        }
    }

    private static async Task<ReadException> NextErrorAsync(PartitionReader reader)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(Token);
        timeout.CancelAfter(PromptLimit);
        while (true)
        {
            try
            {
                await reader.NextAsync(timeout.Token);
            }
            catch (ReadException error)
            {
                return error;
            }
        }
    }

    private static async Task UntilAsync(Func<bool> predicate, long? limitMs = null)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(Token);
        timeout.CancelAfter(limitMs is { } limit ? TimeSpan.FromMilliseconds(limit) : PromptLimit);
        while (!predicate())
        {
            await Task.Delay(5, timeout.Token);
        }
    }

    /// <summary>
    ///     The log of partition 1 up to <see cref="End" /> behind a client that reads it with polls only. A poll
    ///     serves its count from its start, and a committing one stores its last offset. The committing poll after
    ///     <see cref="LoseNextCommittingRead" /> stores, loses its reply and finds the log grown to 20 messages.
    /// </summary>
    private sealed class ScriptedLog
    {
        private readonly object _gate = new();
        private int _recoveryReads;
        private ulong? _stored;

        internal ulong End { get; set; }

        internal bool LoseNextCommittingRead { get; set; }

        /// <summary>Offsets whose message does not decode, so a reply that holds one fails to decode.</summary>
        internal HashSet<ulong> Malformed { get; } = [];

        /// <summary>A read by offset without a commit finds nothing.</summary>
        internal bool RecoveryEmpty { get; init; }

        internal int RecoveryReads => Volatile.Read(ref _recoveryReads);

        internal IIggyClient Client()
        {
            var client = new Mock<IIggyClient>();
            client.Setup(instance => instance.PollMessagesAsync(Stream, Topic, 1, It.IsAny<Consumer>(),
                    It.IsAny<PollingStrategy>(), It.IsAny<PollOptions>(), It.IsAny<CancellationToken>()))
                .Returns<Identifier, Identifier, uint, Consumer, PollingStrategy, PollOptions, CancellationToken>(
                    (_, _, _, _, start, options, _) => Poll(start, options));
            return client.Object;
        }

        private Task<PolledMessages> Poll(PollingStrategy start, PollOptions options)
        {
            lock (_gate)
            {
                var from = start.Kind == MessagePolling.Next ? _stored + 1 ?? 0 : start.Value;
                var until = Math.Min(End, from + options.Count);
                var commits = options.Commit.Mode == CommitMode.OnRead;
                if (start.Kind == MessagePolling.Offset && !commits)
                {
                    Interlocked.Increment(ref _recoveryReads);
                    until = RecoveryEmpty ? from : until;
                }

                MessageResponse[] messages =
                [
                    .. Enumerable.Range(0, (int)(until > from ? until - from : 0)).Select(index =>
                        new MessageResponse { Header = new MessageHeader { Offset = from + (ulong)index }, Payload = [1] })
                ];
                if (commits && messages.Length > 0)
                {
                    _stored = until - 1;
                    if (LoseNextCommittingRead)
                    {
                        LoseNextCommittingRead = false;
                        End = 20;
                        return Task.FromException<PolledMessages>(new VsrFetchProgressUncertainException(7));
                    }
                }

                if (messages.Any(message => Malformed.Contains(message.Header.Offset)))
                {
                    return Task.FromException<PolledMessages>(new MalformedResponseException("The batch does not decode."));
                }

                return Task.FromResult(new PolledMessages
                {
                    PartitionId = 1,
                    CurrentOffset = End - 1,
                    Context = ReaderCluster.Served,
                    Messages = messages
                });
            }
        }
    }
}
