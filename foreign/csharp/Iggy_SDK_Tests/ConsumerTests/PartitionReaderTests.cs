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
using System.Diagnostics;
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

    /// <summary>How long a test waits to see that no further read starts.</summary>
    private static readonly TimeSpan SettleWindow = TimeSpan.FromMilliseconds(200);

    /// <summary>The longest pause between two syncs of the assignment of a group reader.</summary>
    private static readonly TimeSpan GroupRefreshInterval = TimeSpan.FromSeconds(1);

    /// <summary>At most this many reads of one reader run at once.</summary>
    private const int ReaderRequests = 32;

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

    [Theory]
    [InlineData(1u, false, 32)]
    [InlineData(64u, false, 64)]
    [InlineData(1u, true, 32)]
    public async Task given_prefetch_budgets_when_buffering_should_apply_a_thirty_two_batch_floor(
        uint requestedBatches, bool byteLimited, int expectedBatches)
    {
        const uint MessagesPerBatch = 2;
        using var cluster = new ReaderCluster(1)
        {
            Limit = (ulong)expectedBatches * MessagesPerBatch,
            ReportsHead = true
        };
        using var client = await cluster.ConnectAsync();
        await using var reader = await client.Reader(Stream, Topic, Consumer.New(4))
            .WithPartitions(0)
            .WithStart(PollingStrategy.Offset(0))
            .WithOptions(new PollOptions { Count = MessagesPerBatch, MaxBytes = uint.MaxValue })
            .WithPrefetch(byteLimited ? 1u : uint.MaxValue,
                byteLimited ? uint.MaxValue : requestedBatches * MessagesPerBatch)
            .WithRetryInterval(RetryInterval)
            .BuildAsync(Token);

        await UntilAsync(() => cluster.Fetches.Count >= expectedBatches);

        var batches = await ReadAsync(reader, expectedBatches);
        Assert.Equal(Enumerable.Range(0, expectedBatches).Select(index => (ulong)index * MessagesPerBatch),
            batches.Select(batch => batch.Messages[0].Header.Offset));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task given_one_batch_budget_and_three_primaries_when_two_wait_should_drain_the_ready_primary(
        bool onRead)
    {
        const int Primaries = 3;
        const uint BusyPartition = 2;
        const ulong Messages = 8;
        var options = new PollOptions
        {
            Count = 2,
            MaxWait = TimeSpan.FromSeconds(30),
            Commit = onRead ? CommitPolicy.OnRead : CommitPolicy.Manual
        };
        using var cluster = new ReaderCluster(Primaries)
        {
            Limit = Messages,
            ReportsHead = true,
            HoldFetches = true
        };
        cluster.Stored[0] = Messages - 1;
        cluster.Stored[1] = Messages - 1;
        using var client = await cluster.ConnectAsync();
        var reader = await client.Reader(Stream, Topic, Consumer.New(4))
            .WithPartitions(0, 1, BusyPartition)
            .WithStart(PollingStrategy.Next().WithContext(ReaderCluster.Served))
            .WithOptions(options)
            .WithPrefetch(options.MaxBytes, options.Count)
            .BuildAsync(Token);
        try
        {
            var first = reader.NextAsync(Token).AsTask();
            await UntilAsync(() => cluster.HeldFetches == Primaries);
            Assert.Equal([0u, 1u, BusyPartition],
                cluster.Held.SelectMany(fetch => fetch.Targets).Select(target => target.PartitionId).Order());
            Assert.All(cluster.Held, fetch => Assert.Equal(onRead, fetch.AutoCommit));

            cluster.HoldFetches = false;
            await cluster.ReleaseHeldAsync();

            List<PolledMessages> batches = [await first.WaitAsync(PromptLimit, Token)];
            batches.AddRange(await ReadAsync(reader, (int)(Messages / options.Count) - 1));
            Assert.All(batches, batch => Assert.Equal(BusyPartition, batch.PartitionId));
            Assert.Equal(Enumerable.Range(0, (int)Messages).Select(offset => (ulong)offset),
                batches.SelectMany(batch => batch.Messages).Select(message => message.Header.Offset));
            Assert.Single(cluster.Fetches, fetch => fetch.Node == 0);
            Assert.Single(cluster.Fetches, fetch => fetch.Node == 1);
            Assert.Equal(onRead, cluster.Stored.ContainsKey(BusyPartition));
        }
        finally
        {
            cluster.HoldFetches = false;
            await cluster.ReleaseHeldAsync();
            await reader.CloseAsync(Token);
        }
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

        await reader.CloseAsync(Token);

        var other = taken.PartitionId == 1 ? 2u : 1u;
        Assert.Equal(1ul, cluster.Stored[taken.PartitionId]);
        Assert.False(cluster.Stored.ContainsKey(other));
    }

    /// <summary>
    ///     A read from the start would store its end, and the next read from Next would go on from the stored offset
    ///     5. With on read commits a partition with a stored offset resumes after it instead.
    /// </summary>
    [Theory]
    [InlineData(MessagePolling.Offset)]
    [InlineData(MessagePolling.First)]
    public async Task given_on_read_with_a_stored_offset_when_starting_at_offset_zero_should_resume_after_the_stored_offset(
        MessagePolling kind)
    {
        using var cluster = new ReaderCluster(1) { Limit = 10 };
        cluster.Stored[1] = 5;
        using var client = await cluster.ConnectAsync();
        await using var reader = await Reader(client, CommitPolicy.OnRead, 1)
            .WithStart(new PollingStrategy { Kind = kind, Value = 0 }).BuildAsync(Token);

        var batch = (await ReadAsync(reader, 1))[0];

        Assert.Equal([6ul, 7ul], batch.Messages.Select(message => message.Header.Offset));
        Assert.All(cluster.Fetches.SelectMany(fetch => fetch.Request.Targets),
            target => Assert.Equal(MessagePolling.Next, target.Kind));
        Assert.All(cluster.Polls, poll => Assert.Equal(MessagePolling.Next, poll.Kind));
    }

    [Fact]
    public async Task given_on_read_without_a_stored_offset_when_starting_at_an_offset_should_look_it_up_and_read_from_the_offset()
    {
        using var cluster = new ReaderCluster(1) { Limit = 10 };
        using var client = await cluster.ConnectAsync();
        await using var reader = await Reader(client, CommitPolicy.OnRead, 1).WithStart(PollingStrategy.Offset(4))
            .BuildAsync(Token);

        var batch = (await ReadAsync(reader, 1))[0];

        Assert.Equal([4ul, 5ul], batch.Messages.Select(message => message.Header.Offset));
        Assert.Equal([1u], cluster.Lookups);
    }

    /// <summary>
    ///     A start lookup takes a request as a poll does, so more partitions than requests look up their offsets in
    ///     turns. A partition whose lookup did not fit looks up once the others end.
    /// </summary>
    [Fact]
    public async Task given_on_read_with_an_offset_start_and_more_partitions_than_requests_when_looking_up_should_keep_the_request_cap()
    {
        const int Looked = ReaderRequests + 2;
        using var cluster = new ReaderCluster(1) { HoldLookups = true };
        using var client = await cluster.ConnectAsync();
        var reader = await Reader(client, CommitPolicy.OnRead, Partitions(Looked)).WithStart(PollingStrategy.Offset(0))
            .BuildAsync(Token);
        try
        {
            _ = reader.NextAsync(Token).AsTask();
            await UntilAsync(() => cluster.HeldLookups > 0);
            await Task.Delay(SettleWindow, Token);
            Assert.InRange(cluster.HeldLookups, 1, ReaderRequests);

            cluster.HoldLookups = false;
            await cluster.ReleaseHeldLookupsAsync();

            await UntilAsync(() => cluster.Lookups.Count == Looked);
            Assert.Equal(Partitions(Looked), cluster.Lookups.Order());
        }
        finally
        {
            cluster.HoldLookups = false;
            await cluster.ReleaseHeldLookupsAsync();
            await reader.CloseAsync(Token);
        }
    }

    /// <summary>
    ///     A failed start lookup takes the path of a failed read: the application gets its error, the partition reads
    ///     nothing, and it looks up again after the retry interval. The stored offset that the lookup then finds wins
    ///     over the start.
    /// </summary>
    [Fact]
    public async Task given_on_read_with_an_offset_start_when_the_lookup_fails_should_report_it_and_look_up_again()
    {
        using var cluster = new ReaderCluster(1) { Limit = 4, LookupRefusal = VsrError.INVALID_FORMAT };
        cluster.Stored[1] = 1;
        using var client = await cluster.ConnectAsync();
        var reader = await Reader(client, CommitPolicy.OnRead, 1).WithStart(PollingStrategy.Offset(0))
            .BuildAsync(Token);
        try
        {
            var error = await NextErrorAsync(reader);

            Assert.Equal((1u, VsrError.INVALID_FORMAT),
                (error.PartitionId, Assert.IsType<IggyInvalidStatusCodeException>(error.InnerException).StatusCode));
            Assert.Equal((0, 0), (cluster.Fetches.Count, cluster.Polls.Count));
            var refused = cluster.Lookups.Count;
            cluster.LookupRefusal = null;
            var resumed = (await ReadAsync(reader, 1))[0];
            Assert.Equal([2ul, 3ul], resumed.Messages.Select(message => message.Header.Offset));
            Assert.True(cluster.Lookups.Count > refused);
        }
        finally
        {
            await reader.CloseAsync(Token);
        }
    }

    /// <summary>
    ///     The server stored the read ahead of both partitions from the start, and the application took one batch.
    ///     Whatever the start, the close moves the other partition before its undelivered batch. At offset 0 it looks
    ///     the offset up and deletes it.
    /// </summary>
    [Theory]
    [InlineData(0ul)]
    [InlineData(2ul)]
    public async Task given_on_read_with_an_offset_start_when_closing_with_an_undelivered_batch_should_move_its_offset_before_it(
        ulong start)
    {
        using var cluster = new ReaderCluster(1) { Limit = 6, HoldFetches = true };
        using var client = await cluster.ConnectAsync();
        var reader = await Reader(client, CommitPolicy.OnRead, 1, 2).WithStart(PollingStrategy.Offset(start))
            .BuildAsync(Token);
        var first = reader.NextAsync(Token).AsTask();
        await UntilAsync(() => HeldTargets(cluster).Count() == 2);
        cluster.HoldFetches = false;
        await cluster.ReleaseHeldAsync();
        var taken = await first.WaitAsync(PromptLimit, Token);

        await reader.CloseAsync(Token);

        var other = taken.PartitionId == 1 ? 2u : 1u;
        ulong? rewound = start == 0 ? null : start - 1;
        uint[] deletes = start == 0 ? [other] : [];
        Assert.Equal(start + 1, cluster.Stored[taken.PartitionId]);
        Assert.Equal(rewound, cluster.Stored.TryGetValue(other, out var stored) ? stored : null);
        Assert.Equal(deletes, cluster.Deletes);
    }

    /// <summary>
    ///     The committing read from offset 0 ends with an unknown outcome and stored nothing. The close looks the
    ///     offset up before it moves the partition before that read, and sends no delete for the missing offset.
    /// </summary>
    [Fact]
    public async Task given_on_read_when_closing_at_offset_zero_without_a_stored_offset_should_send_no_delete()
    {
        using var cluster = new ReaderCluster(1) { Limit = 4 };
        cluster.UncertainSections.Add(1);
        using var client = await cluster.ConnectAsync();
        var reader = await Reader(client, CommitPolicy.OnRead, 1).BuildAsync(Token);
        using var waiting = CancellationTokenSource.CreateLinkedTokenSource(Token);
        var next = reader.NextAsync(waiting.Token).AsTask();
        await UntilAsync(() => cluster.Fetches.Any(fetch => fetch.Request.AutoCommit));
        await waiting.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => next);

        await reader.CloseAsync(Token);

        Assert.Equal([1u], cluster.Lookups);
        Assert.Empty(cluster.Deletes);
    }

    /// <summary>
    ///     The committing read from a start past the end stored nothing. The close looks the offset up and stores
    ///     nothing, because the offset before the start lies past the end, which the server refuses.
    /// </summary>
    [Fact]
    public async Task given_on_read_with_an_offset_start_past_the_end_when_closing_before_any_message_should_store_nothing()
    {
        using var cluster = new ReaderCluster(1) { Limit = 4 };
        using var client = await cluster.ConnectAsync();
        var reader = await Reader(client, CommitPolicy.OnRead, 1).WithStart(PollingStrategy.Offset(10))
            .BuildAsync(Token);
        using var waiting = CancellationTokenSource.CreateLinkedTokenSource(Token);
        var next = reader.NextAsync(waiting.Token).AsTask();
        await UntilAsync(() => cluster.Fetches.Any(fetch => fetch.Request.AutoCommit));
        await waiting.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => next);

        await reader.CloseAsync(Token);

        Assert.False(cluster.Stored.ContainsKey(1));
    }


    /// <summary>
    ///     The stored offset is 9, the last one. A seek of partition 1 to 8 holds its store while the partition leaves
    ///     the assignment and comes back, and the store then lands with an unknown outcome. The restore point 9 belongs
    ///     to the earlier assignment, so the close does not store it, and the landed offset 7 stays.
    /// </summary>
    [Fact]
    public async Task given_on_read_with_a_seek_store_held_across_a_leave_and_a_return_when_it_has_an_unknown_outcome_should_not_restore_it_at_close()
    {
        using var cluster = new ReaderCluster(1) { Limit = 10, Assignment = [1] };
        cluster.Stored[1] = 9;
        using var client = await cluster.ConnectAsync();
        var reader = await client.Reader(Stream, Topic, Consumer.Group("readers"))
            .WithOptions(new PollOptions { MaxWait = TimeSpan.FromMilliseconds(100), Commit = CommitPolicy.OnRead })
            .WithRetryInterval(RetryInterval).BuildAsync(Token);
        try
        {
            Task seek;
            using (var waiting = CancellationTokenSource.CreateLinkedTokenSource(Token))
            {
                var next = reader.NextAsync(waiting.Token).AsTask();
                await UntilAsync(() => reader.Partitions.Count == 1);
                cluster.HoldStores = true;
                seek = reader.SeekAsync(1, PollingStrategy.Offset(8), Token);
                await UntilAsync(() => cluster.HeldStores == 1);
                cluster.Assignment = [];
                await UntilAsync(() => reader.Partitions.Count == 0, ConsumerGroupClientState.AssignmentRefreshMs * 2);
                cluster.Assignment = [1];
                await UntilAsync(() => reader.Partitions.Count == 1, ConsumerGroupClientState.AssignmentRefreshMs * 2);
                await waiting.CancelAsync();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => next);
            }

            cluster.HoldStores = false;
            cluster.LoseNextStoreReply = true;
            await cluster.ReleaseHeldStoresAsync();
            await Assert.ThrowsAsync<VsrRequestOutcomeUnknownException>(() => seek.WaitAsync(PromptLimit, Token));
        }
        finally
        {
            await reader.CloseAsync(Token);
        }

        Assert.Equal((ulong?)7, cluster.Stored.TryGetValue(1, out var stored) ? stored : null);
        Assert.Empty(cluster.Deletes);
    }

    /// <summary>
    ///     The stored offset is 3. The read from 4 stores 5 and loses its reply, and the cluster refuses the reads after
    ///     it. A seek to 2 stores 1 with an unknown outcome. Nothing was delivered, so the close stores the rewind point
    ///     3 as it is. With a lookup first, the close would find 1 and keep it, and a restart would read 2 and 3 again.
    /// </summary>
    [Fact]
    public async Task given_on_read_after_a_lost_read_when_a_seek_store_has_an_unknown_outcome_should_store_the_rewind_point_at_close()
    {
        using var cluster = new ReaderCluster(1) { Limit = 10, LoseNextCommittingRead = true };
        cluster.Stored[1] = 3;
        cluster.OnLostCommittingRead = () => cluster.HoldFetches = true;
        using var client = await cluster.ConnectAsync();
        var reader = await Reader(client, CommitPolicy.OnRead, 1).BuildAsync(Token);
        try
        {
            using (var waiting = CancellationTokenSource.CreateLinkedTokenSource(Token))
            {
                var next = reader.NextAsync(waiting.Token).AsTask();
                await UntilAsync(() => cluster.HeldFetches == 1, (long)LostReplyLimit.TotalMilliseconds);
                await waiting.CancelAsync();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => next);
            }

            cluster.Unauthorized.Add(1);
            cluster.LoseNextStoreReply = true;
            var seek = reader.SeekAsync(1, PollingStrategy.Offset(2), Token);
            cluster.HoldFetches = false;
            await cluster.ReleaseHeldAsync();
            await Assert.ThrowsAsync<VsrRequestOutcomeUnknownException>(() => seek.WaitAsync(PromptLimit, Token));
        }
        finally
        {
            await reader.CloseAsync(Token);
        }

        Assert.Equal((ulong?)3, cluster.Stored.TryGetValue(1, out var stored) ? stored : null);
        Assert.Empty(cluster.Deletes);
    }

    /// <summary>
    ///     The join of the build lands, but its reply is lost. The build fails with the unknown outcome and releases the
    ///     join in the client, so the next metadata write of the client goes out.
    /// </summary>
    [Fact]
    public async Task given_a_group_reader_when_the_join_of_the_build_has_an_unknown_outcome_should_release_it()
    {
        using var cluster = new ReaderCluster(1)
        {
            Assignment = [1],
            LoseNextGroupReply = VsrOperation.JoinConsumerGroup
        };
        using var client = await cluster.ConnectAsync();

        await Assert.ThrowsAsync<VsrRequestOutcomeUnknownException>(async () =>
            await client.Reader(Stream, Topic, Consumer.Group("readers")).BuildAsync(Token));
        await client.JoinConsumerGroupAsync(Stream, Topic, Identifier.String("readers"), Token);

        Assert.Equal(2, cluster.Joins);
    }

    /// <summary>
    ///     The leave of the close lands, but its reply is lost. The close releases it in the client, so the next metadata
    ///     write of the client goes out.
    /// </summary>
    [Fact]
    public async Task given_a_group_reader_when_the_leave_of_the_close_has_an_unknown_outcome_should_release_it()
    {
        using var cluster = new ReaderCluster(1) { Assignment = [1] };
        using var client = await cluster.ConnectAsync();
        var reader = await client.Reader(Stream, Topic, Consumer.Group("readers")).WithRetryInterval(RetryInterval)
            .BuildAsync(Token);
        cluster.LoseNextGroupReply = VsrOperation.LeaveConsumerGroup;

        await reader.CloseAsync(Token);
        await client.JoinConsumerGroupAsync(Stream, Topic, Identifier.String("readers"), Token);

        Assert.Equal((2, 1), (cluster.Joins, cluster.Leaves));
    }

    /// <summary>
    ///     No seek wrote. The first read from the start 4 stores 5 and loses its reply, and the read after it waits.
    ///     Nothing was delivered, so the close looks the offset up and moves it back before the start, without a
    ///     delete.
    /// </summary>
    [Fact]
    public async Task given_on_read_with_an_offset_start_without_a_stored_offset_when_closing_after_a_lost_read_should_move_the_offset_before_the_start()
    {
        using var cluster = new ReaderCluster(1) { Limit = 10, LoseNextCommittingRead = true };
        cluster.OnLostCommittingRead = () => cluster.HoldFetches = true;
        using var client = await cluster.ConnectAsync();
        var reader = await Reader(client, CommitPolicy.OnRead, 1).WithStart(PollingStrategy.Offset(4))
            .BuildAsync(Token);
        using (var waiting = CancellationTokenSource.CreateLinkedTokenSource(Token))
        {
            var next = reader.NextAsync(waiting.Token).AsTask();
            await UntilAsync(() => cluster.HeldFetches == 1, (long)LostReplyLimit.TotalMilliseconds);
            await waiting.CancelAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => next);
        }

        await reader.CloseAsync(Token);

        Assert.Equal((ulong?)3, cluster.Stored.TryGetValue(1, out var stored) ? stored : null);
        Assert.Empty(cluster.Deletes);
    }

    /// <summary>
    ///     The close moves the undelivered partition back, and its store lands with an unknown outcome. The close
    ///     fails and releases the store in the client, so a later reader of that partition on the same client writes.
    /// </summary>
    [Fact]
    public async Task given_on_read_when_the_close_rewind_store_has_an_unknown_outcome_should_fail_the_close_and_free_the_partition()
    {
        using var cluster = new ReaderCluster(1) { Limit = 6, HoldFetches = true };
        using var client = await cluster.ConnectAsync();
        var reader = await Reader(client, CommitPolicy.OnRead, 1, 2).WithStart(PollingStrategy.Offset(2))
            .BuildAsync(Token);
        var first = reader.NextAsync(Token).AsTask();
        await UntilAsync(() => HeldTargets(cluster).Count() == 2);
        cluster.HoldFetches = false;
        await cluster.ReleaseHeldAsync();
        var other = (await first.WaitAsync(PromptLimit, Token)).PartitionId == 1 ? 2u : 1u;
        cluster.LoseNextStoreReply = true;

        await Assert.ThrowsAsync<VsrRequestOutcomeUnknownException>(() => reader.CloseAsync(Token).AsTask());

        Assert.Equal(1ul, cluster.Stored[other]);
        await using var later = await Reader(client, CommitPolicy.OnRead, other).BuildAsync(Token);
        await later.SeekAsync(other, PollingStrategy.Offset(4), Token);
        Assert.Equal(3ul, cluster.Stored[other]);
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
            await reader.CloseAsync(Token);
        }

        Assert.Equal(7ul, cluster.Stored[1]);
    }

    [Theory]
    [InlineData(MessagePolling.Next, 0ul, true)]
    [InlineData(MessagePolling.Last, 0ul, true)]
    [InlineData(MessagePolling.Timestamp, 10ul, true)]
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
            await reader.CloseAsync(Token);
        }
    }

    /// <summary>
    ///     The partition has a queued batch when the cluster refuses the write of its seek: the store of a seek to
    ///     offset 6 or to the timestamp 6, or the delete of a seek to offset 0. The seek moves nothing, so the next read
    ///     call returns that batch.
    /// </summary>
    [Theory]
    [InlineData(MessagePolling.Offset, 6ul)]
    [InlineData(MessagePolling.Timestamp, 6ul)]
    [InlineData(MessagePolling.Offset, 0ul)]
    public async Task given_on_read_with_a_queued_batch_when_the_seek_write_is_refused_should_fail_the_seek_and_keep_the_batch(
        MessagePolling kind, ulong value)
    {
        using var cluster = new ReaderCluster(1) { Limit = 10 };
        using var client = await cluster.ConnectAsync();
        var reader = await Reader(client, CommitPolicy.OnRead, 1, 2).BuildAsync(Token);
        try
        {
            var taken = await reader.NextAsync(Token);
            var queued = taken.PartitionId == 1 ? 2u : 1u;
            cluster.StoresRefused = cluster.DeletesRefused = true;

            var refused = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() =>
                reader.SeekAsync(queued, new PollingStrategy { Kind = kind, Value = value }, Token));

            cluster.StoresRefused = cluster.DeletesRefused = false;
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
            await reader.CloseAsync(Token);
        }
    }

    /// <summary>
    ///     A committing read never moves the stored offset back, so without the delete the reads from Next after the
    ///     first batch of the replay would go on from the old offset.
    /// </summary>
    [Theory]
    [InlineData(MessagePolling.Offset)]
    [InlineData(MessagePolling.First)]
    public async Task given_on_read_with_a_stored_offset_when_seeking_to_zero_should_delete_it_and_replay_every_message(
        MessagePolling kind)
    {
        using var cluster = new ReaderCluster(1) { Limit = 6 };
        using var client = await cluster.ConnectAsync();
        var reader = await Reader(client, CommitPolicy.OnRead, 1).BuildAsync(Token);
        try
        {
            await ReadAsync(reader, 2);
            Assert.Equal(3ul, cluster.Stored[1]);

            await reader.SeekAsync(1, new PollingStrategy { Kind = kind, Value = 0 }, Token);
            var replay = await ReadAsync(reader, 2);

            Assert.Equal([0ul, 1ul, 2ul, 3ul],
                replay.SelectMany(batch => batch.Messages).Select(message => message.Header.Offset));
            Assert.Equal([1u], cluster.Lookups);
            Assert.Equal([1u], cluster.Deletes);
        }
        finally
        {
            await reader.CloseAsync(Token);
        }
    }

    [Fact]
    public async Task given_on_read_without_a_stored_offset_when_seeking_to_zero_should_look_it_up_and_send_no_delete()
    {
        using var cluster = new ReaderCluster(1) { Limit = 4 };
        using var client = await cluster.ConnectAsync();
        var reader = await Reader(client, CommitPolicy.OnRead, 1).BuildAsync(Token);
        try
        {
            await reader.SeekAsync(1, PollingStrategy.Offset(0), Token);
            var batch = await reader.NextAsync(Token);

            Assert.Equal([1u], cluster.Lookups);
            Assert.Empty(cluster.Deletes);
            Assert.Equal([0ul, 1ul], batch.Messages.Select(message => message.Header.Offset));
        }
        finally
        {
            await reader.CloseAsync(Token);
        }
    }

    [Fact]
    public async Task given_on_read_when_the_seek_delete_is_refused_should_fail_the_seek_and_move_nothing()
    {
        using var cluster = new ReaderCluster(1) { Limit = 6 };
        using var client = await cluster.ConnectAsync();
        var reader = await Reader(client, CommitPolicy.OnRead, 1).BuildAsync(Token);
        try
        {
            await reader.NextAsync(Token);
            cluster.DeletesRefused = true;

            var refused = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() =>
                reader.SeekAsync(1, PollingStrategy.Offset(0), Token));

            cluster.DeletesRefused = false;
            Assert.Equal(VsrError.HISTORY_UNAVAILABLE, refused.StatusCode);
            Assert.Equal(1ul, cluster.Stored[1]);
            var resumed = await reader.NextAsync(Token);
            Assert.Equal([2ul, 3ul], resumed.Messages.Select(message => message.Header.Offset));
        }
        finally
        {
            await reader.CloseAsync(Token);
        }
    }

    /// <summary>
    ///     The seek waits for the committing read in flight before its lookup, and then deletes the offset that the
    ///     read stored.
    /// </summary>
    [Fact]
    public async Task given_on_read_when_seeking_to_zero_while_a_committing_read_is_in_flight_should_look_up_after_that_read()
    {
        using var cluster = new ReaderCluster(1) { Limit = 10, HoldFetches = true };
        using var client = await cluster.ConnectAsync();
        var reader = await Reader(client, CommitPolicy.OnRead, 1).BuildAsync(Token);
        try
        {
            using var waiting = CancellationTokenSource.CreateLinkedTokenSource(Token);
            var next = reader.NextAsync(waiting.Token).AsTask();
            await UntilAsync(() => cluster.HeldFetches == 1);

            var seek = reader.SeekAsync(1, PollingStrategy.Offset(0), Token);

            await Task.Delay(SettleWindow, Token);
            Assert.Equal((0, false), (cluster.Lookups.Count, seek.IsCompleted));
            await waiting.CancelAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => next);
            cluster.HoldFetches = false;
            await cluster.ReleaseHeldAsync();
            await seek.WaitAsync(PromptLimit, Token);
            Assert.Equal([1u], cluster.Deletes);
            var batch = await reader.NextAsync(Token);
            Assert.Equal([0ul, 1ul], batch.Messages.Select(message => message.Header.Offset));
        }
        finally
        {
            cluster.HoldFetches = false;
            await cluster.ReleaseHeldAsync();
            await reader.CloseAsync(Token);
        }
    }

    /// <summary>
    ///     The seek waits for the committing read in flight before its store, and then drops the batch of that read and
    ///     starts the new generation at the sought offset.
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
            await reader.CloseAsync(Token);
        }

        Assert.Equal(7ul, cluster.Stored[1]);
    }

    /// <summary>
    ///     The cluster holds the first request of a seek: the store of offset 5, the lookup of a seek to offset 0, or
    ///     the read of the timestamp. A read that started meanwhile would commit from the old position, and the lane of
    ///     the client queues it only behind a store. So no read of the partition starts until the seek ends, and the
    ///     first read then starts at the sought offset.
    /// </summary>
    [Theory]
    [InlineData(MessagePolling.Offset, 6ul)]
    [InlineData(MessagePolling.Offset, 0ul)]
    [InlineData(MessagePolling.Timestamp, 6ul)]
    public async Task given_on_read_when_a_seek_request_is_held_should_start_no_read_until_the_seek_ends_and_then_read_from_the_sought_offset(
        MessagePolling kind, ulong value)
    {
        using var cluster = new ReaderCluster(1) { Limit = 10 };
        using var client = await cluster.ConnectAsync();
        var reader = await Reader(client, CommitPolicy.OnRead, 1).BuildAsync(Token);
        try
        {
            await reader.NextAsync(Token);
            var fetched = cluster.Fetches.Count;
            var polled = cluster.Polls.Count;
            cluster.HoldStores = cluster.HoldLookups = cluster.HoldPolls = true;

            var seek = reader.SeekAsync(1, new PollingStrategy { Kind = kind, Value = value }, Token);
            await UntilAsync(() => cluster.HeldStores + cluster.HeldLookups + cluster.HeldPolls.Count == 1);
            var next = reader.NextAsync(Token).AsTask();
            await Task.Delay(SettleWindow, Token);

            // A read during the seek shows up as a fetch, a served poll or a second held request.
            Assert.Equal((fetched, polled, 1, false, false),
                (cluster.Fetches.Count, cluster.Polls.Count,
                    cluster.HeldStores + cluster.HeldLookups + cluster.HeldPolls.Count, seek.IsCompleted,
                    next.IsCompleted));
            await ReleaseSeekRequestsAsync(cluster);
            await seek.WaitAsync(PromptLimit, Token);
            var batch = await next.WaitAsync(PromptLimit, Token);
            Assert.Equal([value, value + 1], batch.Messages.Select(message => message.Header.Offset));
            var first = Assert.Single(cluster.Fetches.ElementAt(fetched).Request.Targets);
            Assert.Equal((MessagePolling.Offset, value), (first.Kind, first.Value));
        }
        finally
        {
            await ReleaseSeekRequestsAsync(cluster);
            await reader.CloseAsync(Token);
        }
    }

    /// <summary>
    ///     The cluster holds the lookup of a seek to offset 0 and then refuses its delete. No read of the partition
    ///     starts while the seek runs, the failed seek moves nothing, and the next read goes on from the old position.
    /// </summary>
    [Fact]
    public async Task given_on_read_when_a_held_seek_write_fails_should_start_no_read_meanwhile_and_read_on_from_the_old_position()
    {
        using var cluster = new ReaderCluster(1) { Limit = 10 };
        using var client = await cluster.ConnectAsync();
        var reader = await Reader(client, CommitPolicy.OnRead, 1).BuildAsync(Token);
        try
        {
            await reader.NextAsync(Token);
            var fetched = cluster.Fetches.Count;
            cluster.HoldLookups = true;
            cluster.DeletesRefused = true;

            var seek = reader.SeekAsync(1, PollingStrategy.Offset(0), Token);
            await UntilAsync(() => cluster.HeldLookups == 1);
            var next = reader.NextAsync(Token).AsTask();
            await Task.Delay(SettleWindow, Token);

            Assert.Equal((fetched, false), (cluster.Fetches.Count, next.IsCompleted));
            cluster.HoldLookups = false;
            await cluster.ReleaseHeldLookupsAsync();
            var refused = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => seek);
            Assert.Equal(VsrError.HISTORY_UNAVAILABLE, refused.StatusCode);
            var resumed = await next.WaitAsync(PromptLimit, Token);
            Assert.Equal([2ul, 3ul], resumed.Messages.Select(message => message.Header.Offset));
            Assert.Equal(3ul, cluster.Stored[1]);
        }
        finally
        {
            cluster.HoldLookups = false;
            cluster.DeletesRefused = false;
            await cluster.ReleaseHeldLookupsAsync();
            await reader.CloseAsync(Token);
        }
    }

    /// <summary>
    ///     The caller cancels a seek while the cluster holds its store. The seek returns the cancel at once and ends in
    ///     the background: the reads of the partition stay held, and the reader moves once the store succeeds.
    /// </summary>
    [Fact]
    public async Task given_on_read_when_a_seek_is_canceled_after_its_store_is_sent_should_return_at_once_and_move_after_the_store()
    {
        using var cluster = new ReaderCluster(1) { Limit = 10 };
        using var client = await cluster.ConnectAsync();
        var reader = await Reader(client, CommitPolicy.OnRead, 1).BuildAsync(Token);
        try
        {
            await reader.NextAsync(Token);
            var fetched = cluster.Fetches.Count;
            cluster.HoldStores = true;
            using var canceling = CancellationTokenSource.CreateLinkedTokenSource(Token);
            var seek = reader.SeekAsync(1, PollingStrategy.Offset(6), canceling.Token);
            await UntilAsync(() => cluster.HeldStores == 1);

            await canceling.CancelAsync();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => seek.WaitAsync(PromptLimit, Token));
            var next = reader.NextAsync(Token).AsTask();
            await Task.Delay(SettleWindow, Token);
            Assert.Equal((fetched, 1, false), (cluster.Fetches.Count, cluster.HeldStores, next.IsCompleted));
            await ReleaseSeekRequestsAsync(cluster);
            var batch = await next.WaitAsync(PromptLimit, Token);
            Assert.Equal([6ul, 7ul], batch.Messages.Select(message => message.Header.Offset));
        }
        finally
        {
            await ReleaseSeekRequestsAsync(cluster);
            await reader.CloseAsync(Token);
        }

        Assert.Equal(7ul, cluster.Stored[1]);
    }

    /// <summary>
    ///     The cluster stores 5 for a seek to offset 6, but its reply leaves the outcome unknown. The seek fails and
    ///     keeps the old position. The reader abandons the store, so the partition reads and seeks again, and the next
    ///     read gets 2 and 3 again although the stored offset jumped past them.
    /// </summary>
    [Fact]
    public async Task given_on_read_when_a_seek_store_has_an_unknown_outcome_should_keep_the_old_position_and_free_the_partition()
    {
        using var cluster = new ReaderCluster(1) { Limit = 10 };
        using var client = await cluster.ConnectAsync();
        var reader = await Reader(client, CommitPolicy.OnRead, 1).BuildAsync(Token);
        try
        {
            await reader.NextAsync(Token);
            cluster.LoseNextStoreReply = true;

            await Assert.ThrowsAsync<VsrRequestOutcomeUnknownException>(() =>
                reader.SeekAsync(1, PollingStrategy.Offset(6), Token));

            Assert.Equal(5ul, cluster.Stored[1]);
            var resumed = (await ReadAsync(reader, 1))[0];
            Assert.Equal([2ul, 3ul], resumed.Messages.Select(message => message.Header.Offset));
            await reader.SeekAsync(1, PollingStrategy.Offset(8), Token);
            var sought = (await ReadAsync(reader, 1))[0];
            Assert.Equal([8ul, 9ul], sought.Messages.Select(message => message.Header.Offset));
        }
        finally
        {
            await reader.CloseAsync(Token);
        }
    }

    /// <summary>
    ///     The partition read nothing yet, so its position follows the stored offset 1: a Next start, or an offset start
    ///     whose lookup did not run. The store of a seek to offset 6 lands with an unknown outcome. The reader goes on
    ///     after the offset that it looked up before the store, so it delivers 2 to 5 although the stored offset jumped.
    /// </summary>
    [Theory]
    [InlineData(MessagePolling.Next)]
    [InlineData(MessagePolling.Offset)]
    public async Task given_on_read_before_any_read_when_a_seek_store_has_an_unknown_outcome_should_read_on_after_the_old_stored_offset(
        MessagePolling start)
    {
        using var cluster = new ReaderCluster(1) { Limit = 10, LoseNextStoreReply = true };
        cluster.Stored[1] = 1;
        using var client = await cluster.ConnectAsync();
        var reader = await Reader(client, CommitPolicy.OnRead, 1)
            .WithStart(start == MessagePolling.Next ? PollingStrategy.Next() : PollingStrategy.Offset(0))
            .BuildAsync(Token);
        try
        {
            await Assert.ThrowsAsync<VsrRequestOutcomeUnknownException>(() =>
                reader.SeekAsync(1, PollingStrategy.Offset(6), Token));

            Assert.Equal(5ul, cluster.Stored[1]);
            var delivered = await ReadAsync(reader, 2);
            Assert.Equal([2ul, 3ul, 4ul, 5ul],
                delivered.SelectMany(batch => batch.Messages).Select(message => message.Header.Offset));
        }
        finally
        {
            await reader.CloseAsync(Token);
        }
    }

    /// <summary>
    ///     A Next start before its first read names no offset. The delete of a seek to First lands with an unknown
    ///     outcome. The reader reads on after the stored offset 4 that the seek looked up, and the close stores it back,
    ///     so a restart does not read from 0 again.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task given_on_read_before_any_read_when_a_seek_delete_has_an_unknown_outcome_should_read_on_after_the_old_stored_offset_and_restore_it(
        bool reads)
    {
        using var cluster = new ReaderCluster(1) { Limit = 10, LoseNextDeleteReply = true };
        cluster.Stored[1] = 4;
        using var client = await cluster.ConnectAsync();
        await using var reader = await Reader(client, CommitPolicy.OnRead, 1).WithStart(PollingStrategy.Next())
            .BuildAsync(Token);
        await Assert.ThrowsAsync<VsrRequestOutcomeUnknownException>(() =>
            reader.SeekAsync(1, PollingStrategy.First(), Token));
        Assert.False(cluster.Stored.ContainsKey(1));
        ulong[] resumed = reads
            ? [.. (await ReadAsync(reader, 1))[0].Messages.Select(message => message.Header.Offset)]
            : [];

        await reader.CloseAsync(Token);

        ulong[] expected = reads ? [5, 6] : [];
        Assert.Equal(expected, resumed);
        Assert.Equal((ulong?)(reads ? 6 : 4), cluster.Stored.TryGetValue(1, out var stored) ? stored : null);
    }

    /// <summary>
    ///     The partition ends at the stored offset 4. The delete of a seek to First lands with an unknown outcome, so
    ///     the reader reads on from 5. The reads from 5 find nothing until the partition grows, and the next batch
    ///     starts at 5, not at 0.
    /// </summary>
    [Fact]
    public async Task given_on_read_at_the_end_when_a_seek_delete_has_an_unknown_outcome_should_keep_reading_after_the_old_stored_offset()
    {
        using var cluster = new ReaderCluster(1) { Limit = 5, LoseNextDeleteReply = true };
        cluster.Stored[1] = 4;
        using var client = await cluster.ConnectAsync();
        await using var reader = await Reader(client, CommitPolicy.OnRead, 1).BuildAsync(Token);
        await Assert.ThrowsAsync<VsrRequestOutcomeUnknownException>(() =>
            reader.SeekAsync(1, PollingStrategy.First(), Token));
        var next = reader.NextAsync(Token).AsTask();
        await UntilAsync(() => cluster.Fetches.Count(fetch => fetch.Request.Targets[0].Value == 5) >= 2);
        cluster.Limit = 7;

        var batch = await next.WaitAsync(PromptLimit, Token);

        Assert.Equal([5ul, 6ul], batch.Messages.Select(message => message.Header.Offset));
    }

    /// <summary>
    ///     The start lookups of partitions 1 and 2 wait on the cluster. A seek of partition 1 to offset 6 stores 5 with
    ///     an unknown outcome, so the reader pins the position after the stored offset 1 that the seek looked up. The
    ///     start lookup of partition 1 then finds the stored offset 5. Its result would move the partition to Next and
    ///     skip 2 to 5, so the reader drops it.
    /// </summary>
    [Fact]
    public async Task given_on_read_with_start_lookups_in_flight_when_a_seek_store_has_an_unknown_outcome_should_keep_the_pinned_position()
    {
        using var cluster = new ReaderCluster(1) { Limit = 10, HoldLookups = true };
        cluster.Stored[1] = 1;
        using var client = await cluster.ConnectAsync();
        await using var reader = await Reader(client, CommitPolicy.OnRead, 1, 2).WithStart(PollingStrategy.Offset(0))
            .BuildAsync(Token);
        try
        {
            var first = reader.NextAsync(Token).AsTask();
            await UntilAsync(() => cluster.HeldLookups == 2);
            cluster.HoldLookups = false;
            cluster.LoseNextStoreReply = true;

            await Assert.ThrowsAsync<VsrRequestOutcomeUnknownException>(() =>
                reader.SeekAsync(1, PollingStrategy.Offset(6), Token));
            await cluster.ReleaseHeldLookupsAsync();

            var batch = await first.WaitAsync(PromptLimit, Token);
            batch = batch.PartitionId == 1 ? batch : await NextOfAsync(reader, 1);
            Assert.Equal([2ul, 3ul], batch.Messages.Select(message => message.Header.Offset));
        }
        finally
        {
            cluster.HoldLookups = false;
            await cluster.ReleaseHeldLookupsAsync();
        }
    }

    /// <summary>
    ///     The start lookups of partitions 1 and 2 wait on a cluster without stored offsets. A seek of partition 1 to
    ///     offset 6 looks up no offset either, and the cluster holds its store. The start lookups end during the seek,
    ///     and then the store lands with an unknown outcome. The seek owns the position, so the reader drops the start
    ///     lookup of partition 1 and keeps the start 8. Applied, it would make the pin read from offset 0.
    /// </summary>
    [Fact]
    public async Task given_on_read_with_a_start_lookup_that_ends_during_a_seek_when_the_seek_store_has_an_unknown_outcome_should_keep_the_start()
    {
        using var cluster = new ReaderCluster(1) { Limit = 10, HoldLookups = true };
        using var client = await cluster.ConnectAsync();
        await using var reader = await Reader(client, CommitPolicy.OnRead, 1, 2).WithStart(PollingStrategy.Offset(8))
            .BuildAsync(Token);
        try
        {
            var first = reader.NextAsync(Token).AsTask();
            await UntilAsync(() => cluster.HeldLookups == 2);
            cluster.HoldLookups = false;
            cluster.HoldStores = true;
            var seek = reader.SeekAsync(1, PollingStrategy.Offset(6), Token);
            await UntilAsync(() => cluster.HeldStores == 1);

            await cluster.ReleaseHeldLookupsAsync();
            // Partition 2 reads only after the lookups of both partitions ended.
            var other = await first.WaitAsync(PromptLimit, Token);
            cluster.HoldStores = false;
            cluster.LoseNextStoreReply = true;
            await cluster.ReleaseHeldStoresAsync();
            await Assert.ThrowsAsync<VsrRequestOutcomeUnknownException>(() => seek.WaitAsync(PromptLimit, Token));

            Assert.Equal(2u, other.PartitionId);
            var batch = await NextOfAsync(reader, 1);
            Assert.Equal([8ul, 9ul], batch.Messages.Select(message => message.Header.Offset));
        }
        finally
        {
            cluster.HoldLookups = cluster.HoldStores = false;
            await cluster.ReleaseHeldLookupsAsync();
            await cluster.ReleaseHeldStoresAsync();
        }
    }

    /// <summary>
    ///     Over a client without lanes, the Next start polls its first message without a commit, and that poll waits. A
    ///     seek to offset 6 stores 5 with an unknown outcome, so the reader pins the old position after the stored offset
    ///     1. The waiting poll then finds 6. Its result would replace the pin and skip 2 to 5, so the reader drops it.
    /// </summary>
    [Fact]
    public async Task given_on_read_with_a_start_poll_in_flight_when_a_seek_store_has_an_unknown_outcome_should_keep_the_pinned_position()
    {
        const ulong Sought = 6;
        ulong stored = 1;
        var polling = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var startPoll = new TaskCompletionSource<PolledMessages>(TaskCreationOptions.RunContinuationsAsynchronously);
        var client = new Mock<IIggyClient>();
        client.Setup(instance => instance.PollMessagesAsync(Stream, Topic, 1, It.IsAny<Consumer>(),
                It.IsAny<PollingStrategy>(), It.IsAny<PollOptions>(), It.IsAny<CancellationToken>()))
            .Returns<Identifier, Identifier, uint, Consumer, PollingStrategy, PollOptions, CancellationToken>(
                (_, _, _, _, start, options, _) =>
                {
                    if (start is { Kind: MessagePolling.Next, Context: null })
                    {
                        polling.TrySetResult();
                        return startPoll.Task;
                    }

                    var from = start.Kind == MessagePolling.Next ? stored + 1 : start.Value;
                    if (options.Commit.Mode == CommitMode.OnRead)
                    {
                        stored = Math.Max(stored, from + 1);
                    }

                    return Task.FromResult(Served(from));
                });
        client.Setup(instance => instance.GetOffsetAsync(It.IsAny<Consumer>(), Stream, Topic, 1,
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(() => new OffsetResponse { PartitionId = 1, CurrentOffset = 9, StoredOffset = stored });
        client.Setup(instance => instance.StoreOffsetAsync(It.IsAny<Consumer>(), Stream, Topic, Sought - 1, 1,
                It.IsAny<PartitionContext>(), It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                stored = Sought - 1;
                return Task.FromException(new VsrRequestOutcomeUnknownException(new IOException("The reply was lost.")));
            });
        await using var reader = await client.Object.Reader(Stream, Topic, Consumer.New(4)).WithPartitions(1)
            .WithOptions(new PollOptions { MaxWait = TimeSpan.FromMilliseconds(100), Commit = CommitPolicy.OnRead })
            .WithRetryInterval(RetryInterval).BuildAsync(Token);
        var first = reader.NextAsync(Token).AsTask();
        await polling.Task.WaitAsync(PromptLimit, Token);

        await Assert.ThrowsAsync<VsrRequestOutcomeUnknownException>(() =>
            reader.SeekAsync(1, PollingStrategy.Offset(Sought), Token));
        startPoll.SetResult(Served(Sought));

        var batch = await first.WaitAsync(PromptLimit, Token);
        Assert.Equal([2ul, 3ul], batch.Messages.Select(message => message.Header.Offset));
    }

    /// <summary>
    ///     A committing read of partition 1 is in flight when a rebalance takes the partition away and gives it back.
    ///     The lookup of the new assignment fails, so the new state reads nothing. A seek to offset 0 still waits for
    ///     the old read, or its lookup would miss the offset that the old read then stores.
    /// </summary>
    [Fact]
    public async Task given_on_read_when_seeking_a_partition_assigned_again_should_wait_for_the_read_of_its_old_assignment()
    {
        using var cluster = new ReaderCluster(1) { Limit = 10, Assignment = [1], HoldFetches = true };
        using var client = await cluster.ConnectAsync();
        var reader = await client.Reader(Stream, Topic, Consumer.Group("readers"))
            .WithStart(PollingStrategy.Offset(0))
            .WithOptions(new PollOptions { MaxWait = TimeSpan.FromMilliseconds(100), Commit = CommitPolicy.OnRead })
            .WithRetryInterval(RetryInterval).BuildAsync(Token);
        try
        {
            var first = reader.NextAsync(Token).AsTask();
            await UntilAsync(() => cluster.HeldFetches == 1);
            cluster.LookupRefusal = VsrError.INVALID_FORMAT;
            cluster.Assignment = [];
            await UntilAsync(() => reader.Partitions.Count == 0, ConsumerGroupClientState.AssignmentRefreshMs * 2);
            cluster.Assignment = [1];
            await UntilAsync(() => reader.Partitions.Count == 1, ConsumerGroupClientState.AssignmentRefreshMs * 2);
            await Assert.ThrowsAsync<ReadException>(() => first.WaitAsync(PromptLimit, Token));
            cluster.LookupRefusal = null;

            var seek = reader.SeekAsync(1, PollingStrategy.Offset(0), Token);

            await Task.Delay(SettleWindow, Token);
            Assert.False(seek.IsCompleted);
            cluster.HoldFetches = false;
            await cluster.ReleaseHeldAsync();
            await seek.WaitAsync(PromptLimit, Token);
            Assert.False(cluster.Stored.ContainsKey(1));
        }
        finally
        {
            cluster.HoldFetches = false;
            cluster.LookupRefusal = null;
            await cluster.ReleaseHeldAsync();
            await reader.CloseAsync(Token);
        }
    }

    /// <summary>
    ///     A read from the timestamp would go on from the stored offset 7. The seek finds the offset of the timestamp
    ///     with a read that does not commit and seeks to that offset, so the reader delivers every message from there.
    /// </summary>
    [Theory]
    [InlineData(0ul)]
    [InlineData(2ul)]
    public async Task given_on_read_with_a_stored_offset_ahead_when_seeking_to_a_timestamp_should_read_from_its_offset_without_a_jump(
        ulong timestamp)
    {
        const ulong Messages = 10;
        using var cluster = new ReaderCluster(1) { Limit = Messages };
        cluster.Stored[1] = 7;
        using var client = await cluster.ConnectAsync();
        await using var reader = await Reader(client, CommitPolicy.OnRead, 1).BuildAsync(Token);

        await reader.SeekAsync(1, PollingStrategy.Timestamp(timestamp), Token);
        ulong? sought = timestamp == 0 ? null : timestamp - 1;
        Assert.Equal(sought, cluster.Stored.TryGetValue(1, out var stored) ? stored : null);
        var delivered = await ReadAsync(reader, (int)((Messages - timestamp) / GOLDEN_BATCH_MESSAGES));

        Assert.Equal(Enumerable.Range((int)timestamp, (int)(Messages - timestamp)).Select(offset => (ulong)offset),
            delivered.SelectMany(batch => batch.Messages).Select(message => message.Header.Offset));
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
            await reader.CloseAsync(Token);
        }
    }

    /// <summary>
    ///     A Next start finds its start in the stored offset, and a start by offset applies without one. Either way
    ///     the close moves the offset back before the read that lost its reply.
    /// </summary>
    [Theory]
    [InlineData(MessagePolling.Next, 0ul)]
    [InlineData(MessagePolling.Next, 5ul)]
    [InlineData(MessagePolling.Offset, 0ul)]
    [InlineData(MessagePolling.Offset, 5ul)]
    public async Task given_a_lost_first_reply_when_closing_before_delivery_should_rewind_to_the_captured_start(
        MessagePolling kind, ulong start)
    {
        using var cluster = new ReaderCluster(1) { Limit = start + 4, LoseNextCommittingRead = true };
        if (kind == MessagePolling.Next && start > 0)
        {
            cluster.Stored[1] = start - 1;
        }

        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Token);
        cluster.OnLostCommittingRead = cancellation.Cancel;
        using var client = await cluster.ConnectAsync();
        var reader = await Reader(client, CommitPolicy.OnRead, 1)
            .WithStart(kind == MessagePolling.Next ? PollingStrategy.Next() : PollingStrategy.Offset(start))
            .BuildAsync(Token);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await reader.NextAsync(cancellation.Token));
        await reader.CloseAsync(Token);

        if (start == 0)
        {
            Assert.False(cluster.Stored.ContainsKey(1));
        }
        else
        {
            Assert.Equal(start - 1, cluster.Stored[1]);
        }
    }

    /// <summary>
    ///     A start by timestamp or Last names no offset, and the lookup found no stored offset. A poll that neither
    ///     commits nor delivers finds the first message of the start: for Last with the count of the reader, for a
    ///     timestamp with one message. The committing read from that offset stored and lost its reply, so the close
    ///     moves the offset back before the message, or deletes it at offset 0.
    /// </summary>
    [Theory]
    [InlineData(MessagePolling.Timestamp, 2ul)]
    [InlineData(MessagePolling.Timestamp, 0ul)]
    [InlineData(MessagePolling.Last, 0ul)]
    public async Task given_on_read_with_a_timestamp_or_last_start_when_closing_after_a_lost_first_reply_should_rewind_before_its_first_message(
        MessagePolling kind, ulong timestamp)
    {
        const ulong Messages = 6;
        using var cluster = new ReaderCluster(1) { Limit = Messages, LoseNextCommittingRead = true };
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Token);
        cluster.OnLostCommittingRead = cancellation.Cancel;
        using var client = await cluster.ConnectAsync();
        var reader = await Reader(client, CommitPolicy.OnRead, 1)
            .WithStart(kind == MessagePolling.Timestamp ? PollingStrategy.Timestamp(timestamp) : PollingStrategy.Last())
            .BuildAsync(Token);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await reader.NextAsync(cancellation.Token));

        await reader.CloseAsync(Token);

        var first = kind == MessagePolling.Timestamp ? timestamp : Messages - GOLDEN_BATCH_MESSAGES;
        ulong? rewound = first == 0 ? null : first - 1;
        uint[] deletes = first == 0 ? [1u] : [];
        Assert.Equal(rewound, cluster.Stored.TryGetValue(1, out var stored) ? stored : null);
        Assert.Equal(deletes, cluster.Deletes);
        var found = Assert.Single(cluster.Polls, poll => poll.Kind == kind);
        Assert.Equal((false, kind == MessagePolling.Last ? new PollOptions().Count : 1u), (found.AutoCommit, found.Count));
    }

    /// <summary>
    ///     A seek to Last, or to a timestamp past the end, names no offset and stores nothing. A poll that neither
    ///     commits nor delivers then finds the first message of the position in the grown log. The committing read from
    ///     that offset stored and lost its reply, so the close moves the offset back before the message.
    /// </summary>
    [Theory]
    [InlineData(MessagePolling.Last)]
    [InlineData(MessagePolling.Timestamp)]
    public async Task given_on_read_after_a_seek_that_names_no_offset_when_closing_after_a_lost_reply_should_rewind_before_its_first_message(
        MessagePolling kind)
    {
        const ulong Grown = 10;
        const ulong First = Grown - GOLDEN_BATCH_MESSAGES;
        using var cluster = new ReaderCluster(1) { Limit = 6 };
        using var client = await cluster.ConnectAsync();
        var reader = await Reader(client, CommitPolicy.OnRead, 1).BuildAsync(Token);
        await reader.NextAsync(Token);
        await reader.SeekAsync(1, kind == MessagePolling.Last ? PollingStrategy.Last() : PollingStrategy.Timestamp(First),
            Token);
        Assert.Equal(1ul, cluster.Stored[1]);
        cluster.Limit = Grown;
        cluster.LoseNextCommittingRead = true;
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Token);
        cluster.OnLostCommittingRead = cancellation.Cancel;
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await reader.NextAsync(cancellation.Token));

        await reader.CloseAsync(Token);

        Assert.Equal(First - 1, cluster.Stored[1]);
        Assert.DoesNotContain(cluster.Polls, poll => poll.Kind == kind && poll.AutoCommit);
    }

    /// <summary>
    ///     The partition took a start other than Next, read nothing yet, and has no stored offset. The store of a
    ///     seek lands with an unknown outcome, so the close restores the empty offset with a lookup and a delete, and a
    ///     restart takes the start again. For the start 8, the landed store of 5 would make a restart read from 6.
    /// </summary>
    [Theory]
    [InlineData(MessagePolling.Timestamp)]
    [InlineData(MessagePolling.Last)]
    [InlineData(MessagePolling.Offset)]
    public async Task given_on_read_with_a_start_without_a_stored_offset_when_a_seek_store_has_an_unknown_outcome_should_delete_the_offset_at_close(
        MessagePolling kind)
    {
        using var cluster = new ReaderCluster(1) { Limit = 10, LoseNextStoreReply = true };
        using var client = await cluster.ConnectAsync();
        var reader = await Reader(client, CommitPolicy.OnRead, 1)
            .WithStart(kind switch
            {
                MessagePolling.Timestamp => PollingStrategy.Timestamp(2),
                MessagePolling.Last => PollingStrategy.Last(),
                _ => PollingStrategy.Offset(8)
            })
            .BuildAsync(Token);
        await Assert.ThrowsAsync<VsrRequestOutcomeUnknownException>(() =>
            reader.SeekAsync(1, PollingStrategy.Offset(6), Token));
        Assert.Equal(5ul, cluster.Stored[1]);

        await reader.CloseAsync(Token);

        Assert.False(cluster.Stored.ContainsKey(1));
        Assert.Equal([1u], cluster.Deletes);
    }

    /// <summary>
    ///     The lookup of the start 12 found no stored offset, and the read from 12 found nothing. The store of a seek to
    ///     6 lands with an unknown outcome, so the close restores the empty offset instead of keeping 5 before the start.
    /// </summary>
    [Fact]
    public async Task given_on_read_with_an_offset_start_after_its_lookup_and_an_empty_read_when_a_seek_store_has_an_unknown_outcome_should_delete_the_offset_at_close()
    {
        using var cluster = new ReaderCluster(1) { Limit = 10 };
        using var client = await cluster.ConnectAsync();
        await using var reader = await Reader(client, CommitPolicy.OnRead, 1).WithStart(PollingStrategy.Offset(12))
            .BuildAsync(Token);
        using (var waiting = CancellationTokenSource.CreateLinkedTokenSource(Token))
        {
            var next = reader.NextAsync(waiting.Token).AsTask();
            await UntilAsync(() => !cluster.Fetches.IsEmpty);
            await waiting.CancelAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => next);
        }

        cluster.LoseNextStoreReply = true;
        await Assert.ThrowsAsync<VsrRequestOutcomeUnknownException>(() =>
            reader.SeekAsync(1, PollingStrategy.Offset(6), Token));
        Assert.Equal(5ul, cluster.Stored[1]);

        await reader.CloseAsync(Token);

        Assert.False(cluster.Stored.ContainsKey(1));
        Assert.Equal([1u], cluster.Deletes);
    }

    /// <summary>
    ///     A read of partition 1 from the start 4 stores 5 and queues its batch, which the application does not take.
    ///     The partition leaves the assignment, and it may come back under a new owner generation. The batch of the
    ///     earlier assignment decides nothing, so the close neither moves the offset for it nor fails on its old owner.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task given_on_read_with_a_queued_batch_of_an_earlier_assignment_when_closing_should_neither_rewind_nor_fail(
        bool returns)
    {
        using var cluster = new ReaderCluster(1) { Limit = 10, Assignment = [1], HoldFetches = true };
        using var client = await cluster.ConnectAsync();
        var reader = await client.Reader(Stream, Topic, Consumer.Group("readers"))
            .WithStart(PollingStrategy.Offset(4))
            .WithOptions(new PollOptions { MaxWait = TimeSpan.FromMilliseconds(100), Commit = CommitPolicy.OnRead })
            .WithRetryInterval(RetryInterval).BuildAsync(Token);
        using (var waiting = CancellationTokenSource.CreateLinkedTokenSource(Token))
        {
            var next = reader.NextAsync(waiting.Token).AsTask();
            await UntilAsync(() => cluster.HeldFetches == 1);
            await waiting.CancelAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => next);
        }

        cluster.HoldFetches = false;
        await cluster.ReleaseHeldAsync();
        await UntilAsync(() => cluster.Stored.ContainsKey(1));
        cluster.Assignment = [];
        await UntilAsync(() => reader.Partitions.Count == 0, ConsumerGroupClientState.AssignmentRefreshMs * 2);
        if (returns)
        {
            cluster.OwnerGeneration = ReaderCluster.Served.OwnerGeneration + 1;
            cluster.Assignment = [1];
            await UntilAsync(() => reader.Partitions.Count == 1, ConsumerGroupClientState.AssignmentRefreshMs * 2);
        }

        await reader.CloseAsync(Token);

        Assert.Equal((ulong?)5, cluster.Stored.TryGetValue(1, out var stored) ? stored : null);
        Assert.Empty(cluster.Deletes);
    }

    /// <summary>
    ///     A committing read of partition 1 from the start 4 is in flight when a rebalance takes the partition away and
    ///     gives it back under a new owner generation. The cluster holds the start lookup of the new assignment, so the
    ///     new assignment reads nothing before a seek holds the partition. The seek waits for the old read, which
    ///     stores 5 and queues its batch at 4, and the application may take that batch. The seek store lands with an
    ///     unknown outcome. The batch of the earlier assignment decides nothing, so the close stores 5, the offset from
    ///     before the seek, and deletes nothing.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task given_on_read_with_a_batch_of_an_earlier_assignment_when_a_seek_store_has_an_unknown_outcome_should_store_the_offset_from_before_the_seek_at_close(
        bool delivered)
    {
        using var cluster = new ReaderCluster(1) { Limit = 10, Assignment = [1], HoldFetches = true };
        using var client = await cluster.ConnectAsync();
        var reader = await client.Reader(Stream, Topic, Consumer.Group("readers"))
            .WithStart(PollingStrategy.Offset(4))
            .WithOptions(new PollOptions { MaxWait = TimeSpan.FromMilliseconds(100), Commit = CommitPolicy.OnRead })
            .WithRetryInterval(RetryInterval).BuildAsync(Token);
        try
        {
            using (var waiting = CancellationTokenSource.CreateLinkedTokenSource(Token))
            {
                var first = reader.NextAsync(waiting.Token).AsTask();
                await UntilAsync(() => cluster.HeldFetches == 1);
                cluster.HoldLookupsOf = 1;
                cluster.Assignment = [];
                await UntilAsync(() => reader.Partitions.Count == 0, ConsumerGroupClientState.AssignmentRefreshMs * 2);
                cluster.OwnerGeneration = ReaderCluster.Served.OwnerGeneration + 1;
                cluster.Assignment = [1];
                await UntilAsync(() => cluster.HeldLookups == 1, ConsumerGroupClientState.AssignmentRefreshMs * 2);
                await waiting.CancelAsync();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => first);
            }

            cluster.LoseNextStoreReply = true;
            var seek = reader.SeekAsync(1, PollingStrategy.Offset(8), Token);
            cluster.HoldLookupsOf = null;
            await cluster.ReleaseHeldLookupsAsync();
            cluster.HoldFetches = false;
            await cluster.ReleaseHeldAsync();
            await Assert.ThrowsAsync<VsrRequestOutcomeUnknownException>(() => seek.WaitAsync(PromptLimit, Token));
            if (delivered)
            {
                var batch = await reader.NextAsync(Token).AsTask().WaitAsync(PromptLimit, Token);
                Assert.Equal(4ul, batch.Messages[0].Header.Offset);
            }
        }
        finally
        {
            await reader.CloseAsync(Token);
        }

        Assert.Equal((ulong?)5, cluster.Stored.TryGetValue(1, out var stored) ? stored : null);
        Assert.Empty(cluster.Deletes);
    }

    /// <summary>
    ///     The reader delivered up to 19. A seek to Last writes nothing. Then a seek to 10, or to the first offset,
    ///     writes with an unknown outcome, before any read or after a check poll moved the position to 28 and the
    ///     cluster refused the committing read from there. No position names the stored offset 19, so the seek looks
    ///     it up, and the close stores 19 again.
    /// </summary>
    [Theory]
    [InlineData(10ul, false)]
    [InlineData(0ul, false)]
    [InlineData(10ul, true)]
    [InlineData(0ul, true)]
    public async Task given_on_read_after_a_seek_to_last_when_a_seek_write_has_an_unknown_outcome_should_store_the_offset_from_before_it_at_close(
        ulong target, bool checks)
    {
        using var cluster = new ReaderCluster(1) { Limit = 30 };
        cluster.Stored[1] = 17;
        using var client = await cluster.ConnectAsync();
        var reader = await Reader(client, CommitPolicy.OnRead, 1).BuildAsync(Token);
        try
        {
            var batch = await reader.NextAsync(Token).AsTask().WaitAsync(PromptLimit, Token);
            Assert.Equal(19ul, batch.Messages[^1].Header.Offset);
            await reader.SeekAsync(1, PollingStrategy.Last(), Token);
            if (checks)
            {
                cluster.Unauthorized.Add(1);
                using var waiting = CancellationTokenSource.CreateLinkedTokenSource(Token);
                var next = reader.NextAsync(waiting.Token).AsTask();
                await UntilAsync(() => cluster.Fetches.Any(fetch => fetch.Request.Targets[0].Value == 28));
                await waiting.CancelAsync();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => next);
            }

            cluster.LoseNextStoreReply = target > 0;
            cluster.LoseNextDeleteReply = target == 0;
            await Assert.ThrowsAsync<VsrRequestOutcomeUnknownException>(() =>
                reader.SeekAsync(1, PollingStrategy.Offset(target), Token).WaitAsync(PromptLimit, Token));
        }
        finally
        {
            await reader.CloseAsync(Token);
        }

        Assert.Equal((ulong?)19, cluster.Stored.TryGetValue(1, out var stored) ? stored : null);
    }

    /// <summary>
    ///     The reader delivered up to 19, and a seek to Last writes nothing. A seek to 10 stores 9 and a seek to 25
    ///     stores 24, both with an unknown outcome. The close stores 19, the offset from before the first write, and
    ///     not the offset 9 that the second seek found.
    /// </summary>
    [Fact]
    public async Task given_on_read_after_two_seek_stores_with_an_unknown_outcome_when_closing_should_store_the_offset_from_before_the_first()
    {
        using var cluster = new ReaderCluster(1) { Limit = 30 };
        cluster.Stored[1] = 17;
        using var client = await cluster.ConnectAsync();
        var reader = await Reader(client, CommitPolicy.OnRead, 1).BuildAsync(Token);
        try
        {
            var batch = await reader.NextAsync(Token).AsTask().WaitAsync(PromptLimit, Token);
            Assert.Equal(19ul, batch.Messages[^1].Header.Offset);
            await reader.SeekAsync(1, PollingStrategy.Last(), Token);
            foreach (var target in new ulong[] { 10, 25 })
            {
                cluster.LoseNextStoreReply = true;
                await Assert.ThrowsAsync<VsrRequestOutcomeUnknownException>(() =>
                    reader.SeekAsync(1, PollingStrategy.Offset(target), Token).WaitAsync(PromptLimit, Token));
            }
        }
        finally
        {
            await reader.CloseAsync(Token);
        }

        Assert.Equal((ulong?)19, cluster.Stored.TryGetValue(1, out var stored) ? stored : null);
    }

    /// <summary>
    ///     A group reader delivered 0 and 1. The caller signs out while a committing fetch waits, the cluster stores 2
    ///     and 3, and the reply comes after the session ended. After a new sign-in the reader joins again, and the
    ///     cluster installs the owner again with a new generation. A next read finds the lost read and reads 2 and 3
    ///     again. A close without a next read stores 1. Until a reply of the new session, each read and write carries
    ///     owner generation 0, so the cluster takes it.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task given_on_read_with_a_lost_read_across_a_new_session_should_read_it_again_or_rewind_it_at_close(
        bool reads)
    {
        using var cluster = new ReaderCluster(1) { Limit = 6, Assignment = [1] };
        using var client = await cluster.ConnectAsync();
        var reader = await client.Reader(Stream, Topic, Consumer.Group("readers"))
            .WithOptions(new PollOptions { MaxWait = TimeSpan.FromMilliseconds(100), Commit = CommitPolicy.OnRead })
            .WithRetryInterval(RetryInterval).BuildAsync(Token);
        ulong[] resumed = [];
        try
        {
            Assert.Equal([0ul, 1ul], (await reader.NextAsync(Token)).Messages.Select(message => message.Header.Offset));
            cluster.HoldFetches = true;
            var lost = reader.NextAsync(Token).AsTask();
            await UntilAsync(() => cluster.HeldFetches == 1);
            cluster.HoldLogouts = true;
            var signingOut = client.LogoutUserAsync(Token);
            await UntilAsync(() => cluster.HeldLogouts == 1);
            cluster.HoldFetches = false;
            await cluster.ReleaseHeldAsync();
            await Assert.ThrowsAsync<ReadException>(() => lost.WaitAsync(PromptLimit, Token));
            cluster.HoldLogouts = false;
            await cluster.ReleaseHeldLogoutsAsync();
            await signingOut;
            await SignInAgainAsync(cluster, client);
            if (reads)
            {
                resumed = [.. (await NextBatchAsync(reader)).Messages.Select(message => message.Header.Offset)];
            }
        }
        finally
        {
            await reader.CloseAsync(Token);
        }

        ulong[] expected = reads ? [2, 3] : [];
        Assert.Equal(expected, resumed);
        Assert.Equal((ulong?)(reads ? 3 : 1), cluster.Stored.TryGetValue(1, out var stored) ? stored : null);
        if (!reads)
        {
            // The rewind lands at its first attempt, under owner generation 0 and the metadata op of the join.
            Assert.Equal([0ul], cluster.StoreOwners);
            Assert.Equal([cluster.InstallOp], cluster.StoreOps);
        }
    }

    /// <summary>
    ///     A group reader delivered 0 and 1. After a new sign-in the reader joins again, and the cluster installs the
    ///     owner again with a new generation at a later metadata op. Until a reply of the new session, the store of a
    ///     seek to 4 carries owner generation 0 and the metadata op of the join, so the cluster takes it.
    /// </summary>
    [Fact]
    public async Task given_on_read_after_a_new_session_when_seeking_should_store_under_the_installed_owner()
    {
        using var cluster = new ReaderCluster(1) { Limit = 6, Assignment = [1] };
        using var client = await cluster.ConnectAsync();
        await using var reader = await client.Reader(Stream, Topic, Consumer.Group("readers"))
            .WithOptions(new PollOptions { MaxWait = TimeSpan.FromMilliseconds(100), Commit = CommitPolicy.OnRead })
            .WithRetryInterval(RetryInterval).BuildAsync(Token);
        Assert.Equal([0ul, 1ul], (await reader.NextAsync(Token)).Messages.Select(message => message.Header.Offset));
        await client.LogoutUserAsync(Token);
        await SignInAgainAsync(cluster, client);

        await reader.SeekAsync(1, PollingStrategy.Offset(4), Token);

        Assert.Equal(3ul, cluster.Stored[1]);
        Assert.Equal([0ul], cluster.StoreOwners);
        Assert.Equal([cluster.InstallOp], cluster.StoreOps);
    }

    /// <summary>
    ///     A group reader with the stored offset 3 queues the batch 4 and 5 and delivers nothing. After a new sign-in the
    ///     reader joins again, and the cluster installs the owner again with a new generation. The partition stayed in
    ///     the assignment, so the batch still decides the close, which stores 3 with owner generation 0.
    /// </summary>
    [Fact]
    public async Task given_on_read_with_a_batch_queued_across_a_new_session_when_closing_should_store_before_it()
    {
        using var cluster = new ReaderCluster(1) { Limit = 6, Assignment = [1], HoldFetches = true };
        cluster.Stored[1] = 3;
        using var client = await cluster.ConnectAsync();
        var reader = await client.Reader(Stream, Topic, Consumer.Group("readers"))
            .WithOptions(new PollOptions { MaxWait = TimeSpan.FromMilliseconds(100), Commit = CommitPolicy.OnRead })
            .WithRetryInterval(RetryInterval).BuildAsync(Token);
        try
        {
            using (var waiting = CancellationTokenSource.CreateLinkedTokenSource(Token))
            {
                var next = reader.NextAsync(waiting.Token).AsTask();
                await UntilAsync(() => cluster.HeldFetches == 1);
                await waiting.CancelAsync();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => next);
            }

            cluster.HoldFetches = false;
            await cluster.ReleaseHeldAsync();
            await UntilAsync(() => cluster.Stored[1] == 5);
            await client.LogoutUserAsync(Token);
            await SignInAgainAsync(cluster, client);
        }
        finally
        {
            await reader.CloseAsync(Token);
        }

        Assert.Equal((ulong?)3, cluster.Stored.TryGetValue(1, out var stored) ? stored : null);
        Assert.Equal([0ul], cluster.StoreOwners);
        Assert.Equal([cluster.InstallOp], cluster.StoreOps);
    }

    /// <summary>
    ///     A group reader delivered 0 and 1. Its committing fetch stores 2 and 3 and loses the reply, and the next fetch
    ///     is refused as not owned. The refusal alone is not a leave, so the reader keeps the lost read and reads 2 and
    ///     3 again before 4 and 5.
    /// </summary>
    [Fact]
    public async Task given_on_read_with_a_lost_read_when_a_read_is_refused_as_not_owned_should_read_it_again()
    {
        using var cluster = new ReaderCluster(1) { Limit = 6, Assignment = [1] };
        using var client = await cluster.ConnectAsync();
        await using var reader = await client.Reader(Stream, Topic, Consumer.Group("readers"))
            .WithOptions(new PollOptions { MaxWait = TimeSpan.FromMilliseconds(100), Commit = CommitPolicy.OnRead })
            .WithRetryInterval(RetryInterval).BuildAsync(Token);
        Assert.Equal([0ul, 1ul], (await reader.NextAsync(Token)).Messages.Select(message => message.Header.Offset));
        var lostAt = 0;
        cluster.OnLostCommittingRead = () =>
        {
            cluster.NotOwned.Add(1);
            cluster.HoldPolls = true;
            Volatile.Write(ref lostAt, cluster.Fetches.Count);
        };
        cluster.LoseNextCommittingRead = true;
        var next = NextBatchAsync(reader);
        // After the refusal the partition can leave its fetch group, so its next read is a held poll or a refused fetch.
        await UntilAsync(() => Volatile.Read(ref lostAt) > 0
                               && (cluster.HeldPolls.Count > 0 || cluster.Fetches.Count >= Volatile.Read(ref lostAt) + 2),
            (long)LostReplyLimit.TotalMilliseconds);
        var refused = cluster.Fetches.Count - lostAt;
        cluster.NotOwned.Clear();
        cluster.HoldPolls = false;
        await cluster.ReleaseHeldPollsAsync();

        var resumed = await next;

        Assert.True(refused > 0);
        Assert.Equal([2ul, 3ul], resumed.Messages.Select(message => message.Header.Offset));
    }

    /// <summary>
    ///     A group reader delivered 0 and 1. After a new sign-in the reader joins again, and the cluster installs the
    ///     owner again with a new generation at a later metadata op. The checkpoint of 1 then carries owner generation 0
    ///     and the metadata op of the join, so the cluster takes its commit, its store at the close, or its store when
    ///     the partition leaves.
    /// </summary>
    [Theory]
    [InlineData(true, false)]
    [InlineData(false, false)]
    [InlineData(false, true)]
    public async Task given_a_checkpoint_of_an_earlier_session_when_storing_it_should_store_under_the_installed_owner(
        bool manual, bool leaves)
    {
        using var cluster = new ReaderCluster(1) { Assignment = [1] };
        using var client = await cluster.ConnectAsync();
        var policy = manual
            ? CommitPolicy.Manual
            : CommitPolicy.Auto(new AutoCommit { Interval = TimeSpan.FromHours(1), After = CommitAfter.Delivery });
        var reader = await client.Reader(Stream, Topic, Consumer.Group("readers"))
            .WithOptions(new PollOptions { MaxWait = TimeSpan.FromMilliseconds(100), Commit = policy })
            .WithRetryInterval(RetryInterval).BuildAsync(Token);
        try
        {
            var batch = await reader.NextAsync(Token);
            await client.LogoutUserAsync(Token);
            await SignInAgainAsync(cluster, client);
            if (manual)
            {
                await reader.CommitAsync(batch.Checkpoint!, Token);
            }

            if (leaves)
            {
                cluster.Assignment = [];
                await UntilAsync(() => cluster.Stored.ContainsKey(1), ConsumerGroupClientState.AssignmentRefreshMs * 2);
            }
        }
        finally
        {
            await reader.CloseAsync(Token);
        }

        Assert.Equal(1ul, cluster.Stored[1]);
        Assert.Equal([0ul], cluster.StoreOwners);
        Assert.Equal([cluster.InstallOp], cluster.StoreOps);
    }

    /// <summary>
    ///     A rebalance by another member gave the partition to the reader at a metadata op that the client never saw.
    ///     The store of a seek before any delivery has no context, so the server refuses it as not owned. A read without
    ///     a commit shows the owner, and the store goes again under it.
    /// </summary>
    [Fact]
    public async Task given_on_read_with_an_owner_installed_at_an_unseen_op_when_seeking_should_store_under_the_owner_of_a_read()
    {
        using var cluster = new ReaderCluster(1) { Limit = 6, Assignment = [1] };
        using var client = await cluster.ConnectAsync();
        await using var reader = await client.Reader(Stream, Topic, Consumer.Group("readers"))
            .WithOptions(new PollOptions { MaxWait = TimeSpan.FromMilliseconds(100), Commit = CommitPolicy.OnRead })
            .WithRetryInterval(RetryInterval).BuildAsync(Token);
        await UntilAsync(() => reader.Partitions.Count == 1);
        cluster.InstallOp = ReaderCluster.Served.MetadataOp + 1;

        await reader.SeekAsync(1, PollingStrategy.Offset(4), Token);

        Assert.Equal(3ul, cluster.Stored[1]);
        Assert.Equal([0ul, ReaderCluster.Served.OwnerGeneration], cluster.StoreOwners);
    }

    /// <summary>
    ///     A group reader with the stored offset 3 queues the batch 4 and 5 and delivers nothing. The server refuses the
    ///     store of 3 at the close once as not owned. A read without a commit shows the owner, and the store goes again
    ///     under it.
    /// </summary>
    [Fact]
    public async Task given_on_read_with_a_batch_queued_when_the_close_store_is_refused_once_as_not_owned_should_store_under_the_owner_of_a_read()
    {
        using var cluster = new ReaderCluster(1) { Limit = 6, Assignment = [1], HoldFetches = true };
        cluster.Stored[1] = 3;
        using var client = await cluster.ConnectAsync();
        var reader = await client.Reader(Stream, Topic, Consumer.Group("readers"))
            .WithOptions(new PollOptions { MaxWait = TimeSpan.FromMilliseconds(100), Commit = CommitPolicy.OnRead })
            .WithRetryInterval(RetryInterval).BuildAsync(Token);
        try
        {
            using (var waiting = CancellationTokenSource.CreateLinkedTokenSource(Token))
            {
                var next = reader.NextAsync(waiting.Token).AsTask();
                await UntilAsync(() => cluster.HeldFetches == 1);
                await waiting.CancelAsync();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => next);
            }

            cluster.HoldFetches = false;
            await cluster.ReleaseHeldAsync();
            await UntilAsync(() => cluster.Stored[1] == 5);
            cluster.NextStoreNotOwned = true;
        }
        finally
        {
            await reader.CloseAsync(Token);
        }

        Assert.Equal(3ul, cluster.Stored[1]);
        Assert.Equal([ReaderCluster.Served.OwnerGeneration, ReaderCluster.Served.OwnerGeneration], cluster.StoreOwners);
    }

    /// <summary>
    ///     A partition leaves the assignment, and the server refuses the store of its counted position as not owned.
    ///     The partition left, so the refusal stands: the reader neither reads the partition for the owner nor stores
    ///     again.
    /// </summary>
    [Fact]
    public async Task given_a_partition_that_left_when_its_store_is_refused_as_not_owned_should_neither_read_for_the_owner_nor_store_again()
    {
        using var cluster = new ReaderCluster(1) { Assignment = [1] };
        using var client = await cluster.ConnectAsync();
        var reader = await client.Reader(Stream, Topic, Consumer.Group("readers"))
            .WithOptions(new PollOptions
            {
                MaxWait = TimeSpan.FromMilliseconds(100),
                Commit = CommitPolicy.Auto(new AutoCommit { Interval = TimeSpan.FromHours(1), After = CommitAfter.Delivery })
            })
            .WithRetryInterval(RetryInterval).BuildAsync(Token);
        try
        {
            await reader.NextAsync(Token);
            cluster.NextStoreNotOwned = true;
            cluster.Assignment = [];
            await UntilAsync(() => !cluster.StoreOwners.IsEmpty, ConsumerGroupClientState.AssignmentRefreshMs * 2);
            await Task.Delay(SettleWindow, Token);
        }
        finally
        {
            await reader.CloseAsync(Token);
        }

        Assert.Single(cluster.StoreOwners);
        Assert.Equal(0, cluster.Probes);
        Assert.False(cluster.Stored.ContainsKey(1));
    }

    /// <summary>
    ///     The server refuses a commit as not owned. A refusal alone is not a leave, so the reader asks for the
    ///     assignment again at once, as after such a refusal of a read. The pauses between its syncs have grown to the
    ///     refresh interval by then, so only that request explains a sync right after the refusal.
    /// </summary>
    [Fact]
    public async Task given_a_commit_refused_as_not_owned_should_ask_for_the_assignment_at_once()
    {
        using var cluster = new ReaderCluster(1) { Assignment = [1] };
        using var client = await cluster.ConnectAsync();
        var reader = await client.Reader(Stream, Topic, Consumer.Group("readers"))
            .WithOptions(new PollOptions { MaxWait = TimeSpan.FromMilliseconds(100) })
            .WithRetryInterval(RetryInterval).BuildAsync(Token);
        try
        {
            var batch = await reader.NextAsync(Token);
            await Task.Delay(GroupRefreshInterval * 2, Token);
            var synced = cluster.Syncs;
            await UntilAsync(() => cluster.Syncs > synced, (long)(GroupRefreshInterval * 2).TotalMilliseconds);
            synced = cluster.Syncs;
            cluster.NextStoreNotOwned = true;

            await reader.CommitAsync(batch.Checkpoint!, Token);

            await UntilAsync(() => cluster.Syncs > synced, (long)(GroupRefreshInterval / 4).TotalMilliseconds);
        }
        finally
        {
            await reader.CloseAsync(Token);
        }
    }

    /// <summary>
    ///     The server refuses a commit as not owned and then the read for the owner. The reader reads again after a
    ///     transient refusal, here for the poll quota, until the caller cancels. A read refused as not owned ends at
    ///     once. Either way the commit fails with the refusal of its store, which went out once, and no read for the
    ///     owner goes out after it.
    /// </summary>
    [Theory]
    [InlineData(VsrError.DEFERRED_POLL_CAPACITY_EXCEEDED)]
    [InlineData(VsrError.CONSUMER_GROUP_PARTITION_NOT_OWNED)]
    public async Task given_a_commit_refused_as_not_owned_when_the_read_for_the_owner_is_refused_should_fail_with_the_refusal_of_the_store(
        int refusal)
    {
        using var cluster = new ReaderCluster(1) { Assignment = [1] };
        using var client = await cluster.ConnectAsync();
        var reader = await client.Reader(Stream, Topic, Consumer.Group("readers"))
            .WithOptions(new PollOptions { MaxWait = TimeSpan.FromMilliseconds(100) })
            .WithRetryInterval(RetryInterval).BuildAsync(Token);
        try
        {
            var batch = await reader.NextAsync(Token);
            cluster.NextStoreNotOwned = true;
            cluster.ProbeRefusal = refusal;
            var probes = cluster.Probes;
            using var caller = CancellationTokenSource.CreateLinkedTokenSource(Token);
            var commit = reader.CommitAsync(batch.Checkpoint!, caller.Token);
            if (refusal == VsrError.DEFERRED_POLL_CAPACITY_EXCEEDED)
            {
                await UntilAsync(() => cluster.Probes > probes + 1);
                await caller.CancelAsync();
            }

            var error = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(
                () => commit.WaitAsync(PromptLimit, Token));
            var probed = cluster.Probes;
            await Task.Delay(RetryInterval * 2, Token);

            Assert.Equal(VsrError.CONSUMER_GROUP_PARTITION_NOT_OWNED, error.StatusCode);
            Assert.Single(cluster.StoreOwners);
            Assert.Equal(probed, cluster.Probes);
        }
        finally
        {
            cluster.ProbeRefusal = null;
            await reader.CloseAsync(Token);
        }
    }

    /// <summary>
    ///     A commit is refused as not owned, and the read for the owner ends with an unknown outcome. Only a transient
    ///     refusal makes that read go again, so the commit fails with the refusal of its store after one read.
    /// </summary>
    [Fact]
    public async Task given_a_commit_refused_as_not_owned_when_the_read_for_the_owner_has_an_unknown_outcome_should_fail_with_the_refusal_of_the_store()
    {
        var probes = 0;
        var client = new Mock<IIggyClient>();
        client.Setup(instance => instance.PollMessagesAsync(Stream, Topic, 1, It.IsAny<Consumer>(),
                It.IsAny<PollingStrategy>(), It.IsAny<PollOptions>(), It.IsAny<CancellationToken>()))
            .Returns<Identifier, Identifier, uint, Consumer, PollingStrategy, PollOptions, CancellationToken>(
                (_, _, _, _, start, options, _) =>
                {
                    if (options is not { Count: 1, MaxWait.Ticks: 0 })
                    {
                        return Task.FromResult(Served(start.Value));
                    }

                    Interlocked.Increment(ref probes);
                    return Task.FromException<PolledMessages>(
                        new VsrRequestOutcomeUnknownException(new IOException("The reply was lost.")));
                });
        client.Setup(instance => instance.StoreConsumerPositionAsync(It.IsAny<Consumer>(), Stream, Topic,
                It.IsAny<Checkpoint>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(VsrError.Exception(VsrError.CONSUMER_GROUP_PARTITION_NOT_OWNED,
                "The partition is not owned."));
        await using var reader = await client.Object.Reader(Stream, Topic, Consumer.New(4)).WithPartitions(1)
            .WithOptions(new PollOptions { MaxWait = TimeSpan.FromMilliseconds(100) })
            .WithRetryInterval(RetryInterval).BuildAsync(Token);
        var batch = await reader.NextAsync(Token);

        var error = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(
            () => reader.CommitAsync(batch.Checkpoint!, Token).WaitAsync(PromptLimit, Token));
        await Task.Delay(RetryInterval * 2, Token);

        Assert.Equal(VsrError.CONSUMER_GROUP_PARTITION_NOT_OWNED, error.StatusCode);
        Assert.Equal(1, Volatile.Read(ref probes));
    }

    /// <summary>
    ///     The server refuses a commit as not owned, and then refuses the read for the owner for its poll quota for
    ///     longer than the client retries one read. The token of the commit stays live, so the reader reads again after
    ///     the client gave up, and the store goes again under the owner of the read.
    /// </summary>
    [Fact]
    public async Task given_a_commit_refused_as_not_owned_when_the_read_for_the_owner_hits_the_poll_quota_should_read_again_and_store()
    {
        using var cluster = new ReaderCluster(1) { Assignment = [1] };
        using var client = await cluster.ConnectAsync();
        var reader = await client.Reader(Stream, Topic, Consumer.Group("readers"))
            .WithOptions(new PollOptions { MaxWait = TimeSpan.FromMilliseconds(100) })
            .WithRetryInterval(RetryInterval).BuildAsync(Token);
        try
        {
            var batch = await reader.NextAsync(Token);
            cluster.NextStoreNotOwned = true;
            cluster.ProbeRefusal = VsrError.DEFERRED_POLL_CAPACITY_EXCEEDED;

            var probes = cluster.Probes;
            var commit = reader.CommitAsync(batch.Checkpoint!, Token);
            await UntilAsync(() => cluster.Probes > probes);
            // The client retries one read until its request timeout, which a read without a wait spends on the margin.
            await Task.Delay(new PollOptions().RequestTimeout + SettleWindow, Token);
            cluster.ProbeRefusal = null;
            await commit.WaitAsync(PromptLimit, Token);

            Assert.Equal(batch.Checkpoint!.Offset, cluster.Stored[1]);
            Assert.Equal([ReaderCluster.Served.OwnerGeneration, ReaderCluster.Served.OwnerGeneration],
                cluster.StoreOwners);
        }
        finally
        {
            cluster.ProbeRefusal = null;
            await reader.CloseAsync(Token);
        }
    }

    /// <summary>
    ///     The server refuses the store of the close as not owned, and then every read for the owner for its poll
    ///     quota. A cancel of the token of a later close neither ends those reads nor that close. A cancel of the token
    ///     of the first close ends them, and both closes fail with the refusal of the store after the reader left the
    ///     group. No request goes out after the close.
    /// </summary>
    [Fact]
    public async Task given_on_read_when_every_read_for_the_owner_of_a_refused_close_store_hits_the_poll_quota_should_end_the_close_at_a_cancel_of_its_first_call()
    {
        using var cluster = new ReaderCluster(1) { Limit = 6, Assignment = [1], HoldFetches = true };
        cluster.Stored[1] = 3;
        using var client = await cluster.ConnectAsync();
        var reader = await client.Reader(Stream, Topic, Consumer.Group("readers"))
            .WithOptions(new PollOptions { MaxWait = TimeSpan.FromMilliseconds(100), Commit = CommitPolicy.OnRead })
            .WithRetryInterval(RetryInterval).BuildAsync(Token);
        await QueueFirstBatchAsync(cluster, reader);
        cluster.NextStoreNotOwned = true;
        cluster.ProbeRefusal = VsrError.DEFERRED_POLL_CAPACITY_EXCEEDED;

        var probes = cluster.Probes;
        using var first = CancellationTokenSource.CreateLinkedTokenSource(Token);
        using var second = CancellationTokenSource.CreateLinkedTokenSource(Token);
        var closing = reader.CloseAsync(first.Token).AsTask();
        await UntilAsync(() => cluster.Probes > probes);
        var later = reader.CloseAsync(second.Token).AsTask();
        await second.CancelAsync();
        var probed = cluster.Probes;
        await UntilAsync(() => cluster.Probes > probed);
        Assert.False(closing.IsCompleted || later.IsCompleted);
        await first.CancelAsync();
        var error = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => closing.WaitAsync(PromptLimit, Token));
        var again = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => later.WaitAsync(PromptLimit, Token));
        var sent = (cluster.Polls.Count, cluster.Fetches.Count, cluster.Syncs, cluster.StoreOwners.Count);
        await Task.Delay(RetryInterval * 2, Token);

        Assert.Equal(VsrError.CONSUMER_GROUP_PARTITION_NOT_OWNED, error.StatusCode);
        Assert.Same(error, again);
        Assert.Equal(sent, (cluster.Polls.Count, cluster.Fetches.Count, cluster.Syncs, cluster.StoreOwners.Count));
        Assert.Equal(5ul, cluster.Stored[1]);
        Assert.Equal(1, cluster.Leaves);
    }

    /// <summary>
    ///     The server refuses every store of the close as not owned, and every read for the owner for its poll quota. A
    ///     cancel of the token of the close during those reads ends them, and the refusal stands. The rewind of the
    ///     other partition still goes once, with no read for its owner, and the reader leaves the group. No request
    ///     goes out after the close.
    /// </summary>
    [Fact]
    public async Task given_on_read_when_a_cancel_ends_the_reads_for_the_owner_of_a_refused_close_store_should_still_send_the_other_rewind_once_and_leave()
    {
        using var cluster = new ReaderCluster(1) { Limit = 6, Assignment = [1, 2], HoldFetches = true };
        cluster.Stored[1] = 3;
        cluster.Stored[2] = 3;
        using var client = await cluster.ConnectAsync();
        var reader = await client.Reader(Stream, Topic, Consumer.Group("readers"))
            .WithOptions(new PollOptions { MaxWait = TimeSpan.FromMilliseconds(100), Commit = CommitPolicy.OnRead })
            .WithRetryInterval(RetryInterval).BuildAsync(Token);
        await QueueFirstBatchAsync(cluster, reader);
        cluster.StoresNotOwned = true;
        cluster.ProbeRefusal = VsrError.DEFERRED_POLL_CAPACITY_EXCEEDED;

        var (polls, probes, stores) = (cluster.Polls.Count, cluster.Probes, cluster.StoreOwners.Count);
        using var cancel = CancellationTokenSource.CreateLinkedTokenSource(Token);
        var closing = reader.CloseAsync(cancel.Token).AsTask();
        await UntilAsync(() => cluster.Probes > probes + 1);
        await cancel.CancelAsync();
        var error = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(
            () => closing.WaitAsync(PromptLimit, Token));
        var sent = (cluster.Polls.Count, cluster.Fetches.Count, cluster.Syncs, cluster.StoreOwners.Count);
        await Task.Delay(RetryInterval * 2, Token);

        Assert.Equal(VsrError.CONSUMER_GROUP_PARTITION_NOT_OWNED, error.StatusCode);
        Assert.Equal(sent, (cluster.Polls.Count, cluster.Fetches.Count, cluster.Syncs, cluster.StoreOwners.Count));
        Assert.Equal(stores + 2, cluster.StoreOwners.Count);
        // The rewinds go one at a time, so only the partition of the first one was read for its owner.
        Assert.Single(cluster.Polls.Skip(polls).Select(poll => poll.PartitionId).Distinct());
        Assert.All(cluster.Stored.Values, stored => Assert.Equal(5ul, stored));
        Assert.Equal(1, cluster.Leaves);
    }

    /// <summary>
    ///     The server refuses every store of the close as not owned, and every read for the owner for its poll quota.
    ///     <see cref="PartitionReader.DisposeAsync" /> closes with a token that is never canceled, so the budget of the
    ///     close ends those reads, and the refusal stands. The budget counts once from the start of the close: the
    ///     rewind of the other partition goes once, with no read for its owner, and the reader leaves the group.
    /// </summary>
    [Fact]
    public async Task given_on_read_when_every_read_for_the_owner_of_a_refused_close_store_hits_the_poll_quota_should_end_the_dispose_at_the_close_budget()
    {
        using var cluster = new ReaderCluster(1) { Limit = 6, Assignment = [1, 2], HoldFetches = true };
        cluster.Stored[1] = 3;
        cluster.Stored[2] = 3;
        using var client = await cluster.ConnectAsync();
        var reader = await client.Reader(Stream, Topic, Consumer.Group("readers"))
            .WithOptions(new PollOptions { MaxWait = TimeSpan.FromMilliseconds(100), Commit = CommitPolicy.OnRead })
            .WithRetryInterval(RetryInterval).BuildAsync(Token);
        await QueueFirstBatchAsync(cluster, reader);
        cluster.StoresNotOwned = true;
        cluster.ProbeRefusal = VsrError.DEFERRED_POLL_CAPACITY_EXCEEDED;

        var (polls, stores) = (cluster.Polls.Count, cluster.StoreOwners.Count);
        await reader.DisposeAsync().AsTask().WaitAsync(PartitionReader.CloseOwnerReadBudget + PromptLimit, Token);
        var sent = (cluster.Polls.Count, cluster.Fetches.Count, cluster.Syncs, cluster.StoreOwners.Count);
        await Task.Delay(RetryInterval * 2, Token);

        Assert.Equal(sent, (cluster.Polls.Count, cluster.Fetches.Count, cluster.Syncs, cluster.StoreOwners.Count));
        Assert.Equal(stores + 2, cluster.StoreOwners.Count);
        Assert.Single(cluster.Polls.Skip(polls).Select(poll => poll.PartitionId).Distinct());
        Assert.All(cluster.Stored.Values, stored => Assert.Equal(5ul, stored));
        Assert.Equal(1, cluster.Leaves);
    }

    /// <summary>
    ///     The server refuses the rewind store of the close as not owned, and the read for the owner meets a message
    ///     that does not decode. That read needs only the context of its reply, so the store goes once more under the
    ///     owner of the reply, and the close ends without an error.
    /// </summary>
    [Fact]
    public async Task given_on_read_when_the_read_for_the_owner_of_a_refused_close_store_does_not_decode_should_take_its_context_and_store()
    {
        using var cluster = new ReaderCluster(1) { Limit = 6, Assignment = [1], HoldFetches = true };
        cluster.Stored[1] = 3;
        using var client = await cluster.ConnectAsync();
        var reader = await client.Reader(Stream, Topic, Consumer.Group("readers"))
            .WithOptions(new PollOptions { MaxWait = TimeSpan.FromMilliseconds(100), Commit = CommitPolicy.OnRead })
            .WithRetryInterval(RetryInterval).BuildAsync(Token);
        await QueueFirstBatchAsync(cluster, reader);
        cluster.HoldStores = true;

        var (probes, stores) = (cluster.Probes, cluster.StoreOwners.Count);
        var closing = reader.CloseAsync(Token).AsTask();
        await UntilAsync(() => cluster.HeldStores == 1);
        // The reads ended before the rewind, so only the read for the owner meets the message after 5.
        cluster.Limit = 8;
        cluster.Undecodable.Add(6);
        cluster.NextStoreNotOwned = true;
        cluster.HoldStores = false;
        await cluster.ReleaseHeldStoresAsync();
        await closing.WaitAsync(PromptLimit, Token);

        Assert.Equal(probes + 1, cluster.Probes);
        Assert.Equal(stores + 2, cluster.StoreOwners.Count);
        Assert.Equal(3ul, cluster.Stored[1]);
    }

    /// <summary>
    ///     The server refuses a write that runs in the background as not owned: the store of a seek whose caller left,
    ///     or an automatic store. Then it refuses the read for the owner for its poll quota. The reader is the caller of
    ///     that write, so the close ends the read at once, not after the request timeout of the client. The refusal
    ///     stands, and the write does not go again.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task given_a_write_in_the_background_refused_as_not_owned_when_closing_should_end_the_read_for_the_owner(
        bool seeks)
    {
        using var cluster = new ReaderCluster(1) { Assignment = [1], NextStoreNotOwned = true };
        using var client = await cluster.ConnectAsync();
        var policy = seeks
            ? CommitPolicy.OnRead
            : CommitPolicy.Auto(new AutoCommit { Interval = RetryInterval, After = CommitAfter.Delivery });
        var reader = await client.Reader(Stream, Topic, Consumer.Group("readers"))
            .WithOptions(new PollOptions { MaxWait = TimeSpan.FromMilliseconds(100), Commit = policy })
            .WithRetryInterval(RetryInterval).BuildAsync(Token);
        if (!seeks)
        {
            cluster.ProbeRefusal = VsrError.DEFERRED_POLL_CAPACITY_EXCEEDED;
        }

        await reader.NextAsync(Token).AsTask().WaitAsync(PromptLimit, Token);
        if (seeks)
        {
            // A reader under OnRead finds its start with a read of the same shape, so the refusals start after it.
            cluster.ProbeRefusal = VsrError.DEFERRED_POLL_CAPACITY_EXCEEDED;
            var probes = cluster.Probes;
            using var caller = CancellationTokenSource.CreateLinkedTokenSource(Token);
            var seek = reader.SeekAsync(1, PollingStrategy.Offset(1), caller.Token);
            await UntilAsync(() => cluster.Probes > probes);
            await caller.CancelAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => seek.WaitAsync(PromptLimit, Token));
        }
        else
        {
            await UntilAsync(() => cluster.Probes > 0);
        }

        await reader.CloseAsync(Token).AsTask().WaitAsync(PromptLimit, Token);

        // The close stores the counted position again, and the refusal of the automatic store left the owner unknown.
        ulong[] owners = seeks
            ? [ReaderCluster.Served.OwnerGeneration]
            : [ReaderCluster.Served.OwnerGeneration, 0];
        Assert.Equal(owners, cluster.StoreOwners);
        Assert.Equal(1ul, cluster.Stored[1]);
    }

    /// <summary>
    ///     The server refuses the first store of the close as not owned. The token of the close is canceled while the
    ///     client decodes the reply of the read for the owner, so after that reply. The refusal stands, and that store
    ///     does not go again. The store of the other partition still goes once, the reader leaves the group, and the
    ///     close fails with the refusal.
    /// </summary>
    [Fact]
    public async Task given_auto_with_a_close_store_refused_as_not_owned_when_the_close_is_canceled_after_the_reply_of_the_read_for_the_owner_should_keep_the_refusal_and_still_leave()
    {
        using var cluster = new ReaderCluster(1) { Assignment = [1, 2] };
        var encryptor = new CancelingEncryptor();
        using var client = await cluster.ConnectAsync(encryptor);
        var policy = CommitPolicy.Auto(
            new AutoCommit { Interval = TimeSpan.FromHours(1), After = CommitAfter.Delivery });
        var reader = await client.Reader(Stream, Topic, Consumer.Group("readers"))
            .WithOptions(new PollOptions { MaxWait = TimeSpan.FromMilliseconds(100), Commit = policy })
            .WithRetryInterval(RetryInterval).BuildAsync(Token);
        await ReadAsync(reader, cluster.Assignment.Length);
        cluster.NextStoreNotOwned = true;
        using var close = CancellationTokenSource.CreateLinkedTokenSource(Token);
        encryptor.Cancels = close;

        var (probes, stores) = (cluster.Probes, cluster.StoreOwners.Count);
        var error = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(
            () => reader.CloseAsync(close.Token).AsTask().WaitAsync(PromptLimit, Token));

        Assert.Equal(VsrError.CONSUMER_GROUP_PARTITION_NOT_OWNED, error.StatusCode);
        Assert.True(close.IsCancellationRequested);
        Assert.Equal(probes + 1, cluster.Probes);
        Assert.Equal(stores + 2, cluster.StoreOwners.Count);
        Assert.Single(cluster.Stored);
        Assert.Equal(1, cluster.Leaves);
    }

    /// <summary>
    ///     A commit is refused as not owned. Its caller cancels while the read for the owner waits for its reply, which
    ///     the fake then serves, as when the reply settles just before the cancel. The refusal stands, so the commit
    ///     fails with it, and its store does not go again.
    /// </summary>
    [Fact]
    public async Task given_a_commit_refused_as_not_owned_when_its_caller_cancels_as_the_read_for_the_owner_is_served_should_fail_with_the_refusal_and_not_store_again()
    {
        var stores = 0;
        var probed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var reply = new TaskCompletionSource<PolledMessages>(TaskCreationOptions.RunContinuationsAsynchronously);
        var client = new Mock<IIggyClient>();
        client.Setup(instance => instance.PollMessagesAsync(Stream, Topic, 1, It.IsAny<Consumer>(),
                It.IsAny<PollingStrategy>(), It.IsAny<PollOptions>(), It.IsAny<CancellationToken>()))
            .Returns<Identifier, Identifier, uint, Consumer, PollingStrategy, PollOptions, CancellationToken>(
                (_, _, _, _, start, options, _) =>
                {
                    if (options is not { Count: 1, MaxWait.Ticks: 0 })
                    {
                        return Task.FromResult(Served(start.Value));
                    }

                    probed.TrySetResult();
                    return reply.Task;
                });
        client.Setup(instance => instance.StoreConsumerPositionAsync(It.IsAny<Consumer>(), Stream, Topic,
                It.IsAny<Checkpoint>(), It.IsAny<CancellationToken>()))
            .Returns(() => Interlocked.Increment(ref stores) == 1
                ? Task.FromException(VsrError.Exception(VsrError.CONSUMER_GROUP_PARTITION_NOT_OWNED,
                    "The partition is not owned."))
                : Task.CompletedTask);
        await using var reader = await client.Object.Reader(Stream, Topic, Consumer.New(4)).WithPartitions(1)
            .WithOptions(new PollOptions { MaxWait = TimeSpan.FromMilliseconds(100) })
            .WithRetryInterval(RetryInterval).BuildAsync(Token);
        var batch = await reader.NextAsync(Token);
        using var caller = CancellationTokenSource.CreateLinkedTokenSource(Token);

        var commit = reader.CommitAsync(batch.Checkpoint!, caller.Token);
        await probed.Task.WaitAsync(PromptLimit, Token);
        await caller.CancelAsync();
        reply.SetResult(Served(0));
        var error = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(
            () => commit.WaitAsync(PromptLimit, Token));

        Assert.Equal(VsrError.CONSUMER_GROUP_PARTITION_NOT_OWNED, error.StatusCode);
        Assert.Equal(1, Volatile.Read(ref stores));
    }

    /// <summary>
    ///     A Last start without a stored offset reads from 8. A seek to 6 waits for that committing read, whose batch
    ///     with 8 and 9 stays queued, and the seek store of 5 lands with an unknown outcome. The queued batch decides,
    ///     so the close stores 7 and deletes nothing.
    /// </summary>
    [Fact]
    public async Task given_on_read_with_a_batch_queued_while_a_seek_waits_when_its_store_has_an_unknown_outcome_should_store_before_the_batch_at_close()
    {
        using var cluster = new ReaderCluster(1) { Limit = 10, HoldFetches = true };
        using var client = await cluster.ConnectAsync();
        var reader = await Reader(client, CommitPolicy.OnRead, 1).WithStart(PollingStrategy.Last()).BuildAsync(Token);
        try
        {
            using (var waiting = CancellationTokenSource.CreateLinkedTokenSource(Token))
            {
                var next = reader.NextAsync(waiting.Token).AsTask();
                await UntilAsync(() => cluster.HeldFetches == 1);
                await waiting.CancelAsync();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => next);
            }

            cluster.LoseNextStoreReply = true;
            var seek = reader.SeekAsync(1, PollingStrategy.Offset(6), Token);
            cluster.HoldFetches = false;
            await cluster.ReleaseHeldAsync();
            await Assert.ThrowsAsync<VsrRequestOutcomeUnknownException>(() => seek.WaitAsync(PromptLimit, Token));
        }
        finally
        {
            await reader.CloseAsync(Token);
        }

        Assert.Equal((ulong?)7, cluster.Stored.TryGetValue(1, out var stored) ? stored : null);
        Assert.Empty(cluster.Deletes);
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
    ///     The caller signs out while a committing fetch waits. The server stores the fetch, and its reply comes after
    ///     the session ended, so the client reports the session loss instead of the batch. After a new sign-in, the
    ///     next read finds what the fetch stored and reads it again without a commit.
    /// </summary>
    [Fact]
    public async Task given_on_read_when_a_committing_reply_comes_after_the_session_ended_should_read_its_messages_again()
    {
        using var cluster = new ReaderCluster(1) { Limit = 6 };
        using var client = await cluster.ConnectAsync();
        await using var reader = await Reader(client, CommitPolicy.OnRead, 1).BuildAsync(Token);
        Assert.Equal([0ul, 1ul], (await reader.NextAsync(Token)).Messages.Select(message => message.Header.Offset));
        cluster.HoldFetches = true;
        var lost = reader.NextAsync(Token).AsTask();
        await UntilAsync(() => cluster.HeldFetches == 1);
        cluster.HoldLogouts = true;
        var signingOut = client.LogoutUserAsync(Token);
        await UntilAsync(() => cluster.HeldLogouts == 1);

        cluster.HoldFetches = false;
        await cluster.ReleaseHeldAsync();
        var error = await Assert.ThrowsAsync<ReadException>(() => lost.WaitAsync(PromptLimit, Token));
        cluster.HoldLogouts = false;
        await cluster.ReleaseHeldLogoutsAsync();
        await signingOut;
        await client.LoginUserAsync("user", "secret", Token);
        var resumed = (await ReadAsync(reader, 1, LostReplyLimit))[0];

        Assert.Equal(VsrError.UNAUTHENTICATED,
            Assert.IsAssignableFrom<IggyInvalidStatusCodeException>(error.InnerException).StatusCode);
        Assert.Equal([2ul, 3ul], resumed.Messages.Select(message => message.Header.Offset));
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
    ///     A lost read stored offsets 4 and 5, the next read from Next skipped to 6 and held 6 to 9, and retention then
    ///     deleted the log below 14. The read again from 4 starts at 14, so the reader reports 4 and 5 once, delivers
    ///     the held batch, and reads on from 14.
    /// </summary>
    [Fact]
    public async Task given_on_read_when_retention_deleted_a_lost_range_should_report_it_once_and_deliver_the_held_batch()
    {
        var log = new ScriptedLog { End = 4, RecoveryRetainsFrom = 14 };
        await using var reader = await log.ReaderAsync();
        await reader.NextAsync(Token);
        log.End = 6;
        log.LoseNextCommittingRead = true;

        var lost = await GoneRangeAsync(reader);
        var delivered = await ReadAsync(reader, 2);

        Assert.Contains("offsets 4 to 5", lost.Message);
        Assert.Equal(new OffsetRange(4, 5), lost.Lost);
        Assert.Equal([6ul, 7ul, 8ul, 9ul, 14ul, 15ul, 16ul, 17ul],
            delivered.SelectMany(batch => batch.Messages).Select(message => message.Header.Offset));
    }

    /// <summary>
    ///     A lost read stored offsets 4 and 5, the next read from Next skipped to 6 and held 6 to 9, and retention then
    ///     deleted offset 4. The read again from 4 starts at 5, so the reader reports 4 and reads on from 5.
    /// </summary>
    [Fact]
    public async Task given_on_read_when_retention_deleted_the_start_of_a_lost_range_should_report_that_start_and_read_on()
    {
        var log = new ScriptedLog { End = 4, RecoveryRetainsFrom = 5 };
        await using var reader = await log.ReaderAsync();
        await reader.NextAsync(Token);
        log.End = 6;
        log.LoseNextCommittingRead = true;

        var lost = await GoneRangeAsync(reader);
        var delivered = await ReadAsync(reader, 3);

        Assert.Contains("offsets 4 to 4", lost.Message);
        Assert.Equal(new OffsetRange(4, 4), lost.Lost);
        Assert.Equal(Enumerable.Range(5, 9).Select(offset => (ulong)offset),
            delivered.SelectMany(batch => batch.Messages).Select(message => message.Header.Offset));
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
    ///     The seek waits for the read in flight before its store, and the close waits for the seek before its rewind,
    ///     so the rewind sees the sought position and keeps that offset instead of moving back before the dropped batch.
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

        var closing = reader.CloseAsync(Token).AsTask();
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

        await reader.CloseAsync(Token).AsTask().WaitAsync(PromptLimit, Token);
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
        var first = reader.CloseAsync(Token).AsTask();

        var second = reader.CloseAsync(Token).AsTask();
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

    /// <summary>The reads that the close cancels queue nothing, so the close itself ends a next call without a token.</summary>
    [Fact]
    public async Task given_a_next_call_without_a_token_when_another_task_closes_the_reader_should_end_it_as_closed()
    {
        using var cluster = new ReaderCluster(1) { Limit = 0 };
        using var client = await cluster.ConnectAsync();
        var reader = await Reader(client, CommitPolicy.Manual, 1).BuildAsync(Token);
        var next = reader.NextAsync(CancellationToken.None).AsTask();

        await Task.Run(async () => await reader.CloseAsync(Token), Token);

        await Assert.ThrowsAsync<ObjectDisposedException>(() => next.WaitAsync(PromptLimit, Token));
    }


    /// <summary>A next call that starts after the close ends at once, also without a token.</summary>
    [Fact]
    public async Task given_a_closed_reader_when_a_next_call_starts_should_end_it_as_closed()
    {
        using var cluster = new ReaderCluster(1) { Limit = 0 };
        using var client = await cluster.ConnectAsync();
        var reader = await Reader(client, CommitPolicy.Manual, 1).BuildAsync(Token);
        await reader.CloseAsync(Token);

        await Assert.ThrowsAsync<ObjectDisposedException>(() =>
            reader.NextAsync(CancellationToken.None).AsTask().WaitAsync(PromptLimit, Token));
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

        await reader.CloseAsync(Token);

        Assert.Equal(4ul, cluster.Stored[1]);
    }

    /// <summary>
    ///     The partition moved to another member and back, so the server refuses a read of the old owner generation as
    ///     not owned. The reader keeps its position and reads on with owner generation 0. The refusal lost the old
    ///     owner for good, so the commit of a checkpoint of the old owner carries owner generation 0, and the server
    ///     stores it under the new owner.
    /// </summary>
    [Fact]
    public async Task given_a_new_owner_generation_when_a_read_is_refused_as_not_owned_should_read_on_and_commit_under_the_new_owner()
    {
        using var cluster = new ReaderCluster(1) { Assignment = [1] };
        using var client = await cluster.ConnectAsync();
        var reader = await client.Reader(Stream, Topic, Consumer.Group("readers"))
            .WithOptions(new PollOptions { MaxWait = TimeSpan.FromMilliseconds(100) })
            .WithRetryInterval(RetryInterval).BuildAsync(Token);
        try
        {
            var first = await reader.NextAsync(Token);
            cluster.OwnerGeneration = ReaderCluster.Served.OwnerGeneration + 1;
            cluster.Limit = 4;

            var next = (await ReadAsync(reader, 1))[0];
            await reader.CommitAsync(first.Checkpoint!, Token);

            Assert.Equal([2ul, 3ul], next.Messages.Select(message => message.Header.Offset));
            Assert.Equal(cluster.OwnerGeneration, next.Context.OwnerGeneration);
            Assert.Equal([0ul], cluster.StoreOwners);
            Assert.Equal(1ul, cluster.Stored[1]);
        }
        finally
        {
            await reader.CloseAsync(Token);
        }
    }

    /// <summary>
    ///     An idle partition of an on read group reader gets a new owner generation inside one session. The server
    ///     refuses the read of the old owner as not owned, and the read with owner generation 0 comes back empty under
    ///     the new owner, so the position keeps the context of the old owner. The refusal lost that owner for good, so
    ///     no later read names it.
    /// </summary>
    [Fact]
    public async Task given_on_read_when_an_idle_partition_is_refused_as_not_owned_should_never_name_the_refused_owner_again()
    {
        const int ReadsAfterRefusal = 3;
        using var cluster = new ReaderCluster(1) { Assignment = [1] };
        using var client = await cluster.ConnectAsync();
        var reader = await client.Reader(Stream, Topic, Consumer.Group("readers"))
            .WithOptions(new PollOptions { MaxWait = TimeSpan.FromMilliseconds(100), Commit = CommitPolicy.OnRead })
            .WithRetryInterval(RetryInterval).BuildAsync(Token);
        try
        {
            await reader.NextAsync(Token);
            var before = cluster.Fetches.Count;
            cluster.OwnerGeneration = ReaderCluster.Served.OwnerGeneration + 1;
            using (var waiting = CancellationTokenSource.CreateLinkedTokenSource(Token))
            {
                var next = reader.NextAsync(waiting.Token).AsTask();
                await UntilAsync(() => FetchOwners(cluster, before).SkipWhile(owner => owner != 0).Count()
                                       >= ReadsAfterRefusal);
                await waiting.CancelAsync();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => next);
            }

            var owners = FetchOwners(cluster, before);
            Assert.Equal(ReaderCluster.Served.OwnerGeneration, owners[0]);
            Assert.Equal(Enumerable.Repeat(0ul, ReadsAfterRefusal),
                owners.SkipWhile(owner => owner != 0).Take(ReadsAfterRefusal));
        }
        finally
        {
            await reader.CloseAsync(Token);
        }
    }

    /// <summary>
    ///     The server refuses every read of the partition as not owned. The sixth refusal in a row reports one error,
    ///     and the partition does not stop: once the server serves it again, the reader delivers it without a seek.
    /// </summary>
    [Fact]
    public async Task given_a_partition_that_stays_not_owned_when_reading_should_report_it_after_lasting_refusals_and_read_on()
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
            cluster.NotOwned.Clear();
            var served = await NextBatchAsync(reader);

            Assert.Equal((1u, VsrError.CONSUMER_GROUP_PARTITION_NOT_OWNED),
                (error.PartitionId, Assert.IsType<IggyInvalidStatusCodeException>(error.InnerException).StatusCode));
            Assert.False(error.StopsPartition);
            Assert.Equal([0ul, 1ul], served.Messages.Select(message => message.Header.Offset));
        }
        finally
        {
            await reader.CloseAsync(Token);
        }
    }

    /// <summary>
    ///     The server refuses every read of the partition as not owned, so the wait before each read doubles. A new
    ///     session starts the count and the wait again, as the Rust reader does at a new epoch. So the partition reads
    ///     before the wait of the old session ends, and the wait after its next refusal is one retry interval.
    /// </summary>
    [Fact]
    public async Task given_a_partition_refused_as_not_owned_when_the_session_changes_should_read_at_once_and_wait_one_retry_interval()
    {
        // The doubled wait of the old session must outlast the sync that finds the new session within a second.
        var retryInterval = TimeSpan.FromSeconds(2);
        using var cluster = new ReaderCluster(1) { Assignment = [1] };
        cluster.NotOwned.Add(1);
        using var client = await cluster.ConnectAsync();
        var reader = await client.Reader(Stream, Topic, Consumer.Group("readers"))
            .WithOptions(new PollOptions { MaxWait = TimeSpan.FromMilliseconds(100) })
            .WithRetryInterval(retryInterval).BuildAsync(Token);
        try
        {
            int Reads() => cluster.Fetches.Count + cluster.Polls.Count;

            // After its second refusal in a row the partition waits two retry intervals.
            await UntilAsync(() => Reads() >= 2);
            var refused = Reads();
            var sinceRefusal = Stopwatch.StartNew();
            await client.LogoutUserAsync(Token);
            await SignInAgainAsync(cluster, client);
            await UntilAsync(() => Reads() > refused, (long)(retryInterval * 3).TotalMilliseconds);
            Assert.InRange(sinceRefusal.Elapsed, TimeSpan.Zero, retryInterval * 1.5);

            var sinceRead = Stopwatch.StartNew();
            await UntilAsync(() => Reads() > refused + 1, (long)(retryInterval * 2).TotalMilliseconds);
            Assert.InRange(sinceRead.Elapsed, retryInterval / 2, retryInterval * 1.5);
        }
        finally
        {
            await reader.CloseAsync(Token);
        }
    }

    /// <summary>
    ///     A group member takes the start at each new assignment of a partition. With on read commits it looks up the
    ///     stored offset each time and resumes after it.
    /// </summary>
    [Fact]
    public async Task given_on_read_with_an_offset_start_when_a_member_gets_a_partition_with_a_stored_offset_should_resume_after_it()
    {
        using var cluster = new ReaderCluster(1) { Limit = 8, Assignment = [1] };
        cluster.Stored[1] = 3;
        using var client = await cluster.ConnectAsync();
        var reader = await client.Reader(Stream, Topic, Consumer.Group("readers"))
            .WithStart(PollingStrategy.Offset(0))
            .WithOptions(new PollOptions { MaxWait = TimeSpan.FromMilliseconds(100), Commit = CommitPolicy.OnRead })
            .WithRetryInterval(RetryInterval).BuildAsync(Token);
        try
        {
            Assert.Equal([4ul, 5ul], (await ReadAsync(reader, 1))[0].Messages.Select(message => message.Header.Offset));
            cluster.Assignment = [];
            await UntilAsync(() => reader.Partitions.Count == 0, ConsumerGroupClientState.AssignmentRefreshMs * 2);
            cluster.Assignment = [1];
            await UntilAsync(() => reader.Partitions.Count == 1, ConsumerGroupClientState.AssignmentRefreshMs * 2);

            var again = (await ReadAsync(reader, 1))[0];

            Assert.Equal([6ul, 7ul], again.Messages.Select(message => message.Header.Offset));
            Assert.DoesNotContain(cluster.Fetches.SelectMany(fetch => fetch.Request.Targets),
                target => target.Kind == MessagePolling.Offset);
        }
        finally
        {
            await reader.CloseAsync(Token);
        }
    }

    /// <summary>
    ///     A not owned refusal drops the position of the partition, which takes the start again. With on read commits
    ///     the partition has a stored offset by then, so it resumes after it instead of reading the start twice.
    /// </summary>
    [Fact]
    public async Task given_on_read_with_an_offset_start_when_a_read_is_refused_as_not_owned_should_resume_after_the_stored_offset()
    {
        using var cluster = new ReaderCluster(1) { Assignment = [1] };
        using var client = await cluster.ConnectAsync();
        var reader = await client.Reader(Stream, Topic, Consumer.Group("readers"))
            .WithStart(PollingStrategy.Offset(0))
            .WithOptions(new PollOptions { MaxWait = TimeSpan.FromMilliseconds(100), Commit = CommitPolicy.OnRead })
            .WithRetryInterval(RetryInterval).BuildAsync(Token);
        try
        {
            Assert.Equal([0ul, 1ul], (await ReadAsync(reader, 1))[0].Messages.Select(message => message.Header.Offset));
            cluster.OwnerGeneration = ReaderCluster.Served.OwnerGeneration + 1;
            cluster.Limit = 4;

            var next = (await ReadAsync(reader, 1))[0];

            Assert.Equal([2ul, 3ul], next.Messages.Select(message => message.Header.Offset));
            Assert.Equal(cluster.OwnerGeneration, next.Context.OwnerGeneration);
        }
        finally
        {
            await reader.CloseAsync(Token);
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
            await reader.CloseAsync(Token);
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
            await reader.CloseAsync(Token);
        }
    }

    /// <summary>
    ///     A lost read stores 2 and 3, the next read stores 4 and 5, and the read again of 2 and 3 is in flight when
    ///     partition 1 leaves the assignment. The leave reports 2 and 3 as lost and hands over 4 and 5. The read again
    ///     ends after the leave, and its batch would follow 4 and 5 and the report that its messages are lost.
    /// </summary>
    [Fact]
    public async Task given_on_read_when_a_partition_leaves_while_a_lost_range_is_read_again_should_report_the_range_and_drop_the_late_read()
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
            cluster.HoldPolls = true;
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(Token);
            timeout.CancelAfter(LostReplyLimit);
            var next = reader.NextAsync(timeout.Token).AsTask();
            await UntilAsync(() => cluster.Stored.TryGetValue(1, out var stored) && stored == 5
                                   && cluster.HeldPolls.Count == 1, (long)LostReplyLimit.TotalMilliseconds);
            cluster.Assignment = [];
            await UntilAsync(() => reader.Partitions.Count == 0, ConsumerGroupClientState.AssignmentRefreshMs * 2);
            cluster.HoldPolls = false;
            await cluster.ReleaseHeldPollsAsync();

            var error = await Assert.ThrowsAsync<ReadException>(() => next);
            var lost = Assert.IsAssignableFrom<IggyInvalidStatusCodeException>(error.InnerException);
            Assert.Equal(VsrError.FETCH_PROGRESS_UNCERTAIN, lost.StatusCode);
            Assert.Contains("offsets 2 to 3", lost.Message);
            Assert.Equal(new OffsetRange(2, 3), error.Lost);
            Assert.Equal([4ul, 5ul], (await reader.NextAsync(timeout.Token)).Messages.Select(message => message.Header.Offset));
            using var quiet = CancellationTokenSource.CreateLinkedTokenSource(Token);
            quiet.CancelAfter(SettleWindow);
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => reader.NextAsync(quiet.Token).AsTask());
        }
        finally
        {
            await reader.CloseAsync(Token);
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

    /// <summary>
    ///     Under on read commits, the first read of a partition at Next finds its start without a commit. Its message 0
    ///     does not decode, so its reply tells no offset. The reader takes the start from the same read as a fetch, so
    ///     the message gets a skip, and the seek to the skip resumes after it.
    /// </summary>
    [Fact]
    public async Task given_on_read_with_a_first_message_that_does_not_decode_when_starting_at_next_should_give_a_skip_past_it()
    {
        using var cluster = new ReaderCluster(1) { Limit = 3 };
        cluster.Undecodable.Add(0);
        using var client = await cluster.ConnectAsync();
        await using var reader = await Reader(client, CommitPolicy.OnRead, 1).BuildAsync(Token);

        var error = await Assert.ThrowsAsync<ReadException>(() => reader.NextAsync(Token).AsTask());
        var skip = Assert.NotNull(error.Skip);
        await reader.SeekAsync(1, skip, Token);
        var resumed = await reader.NextAsync(Token);

        Assert.Equal((MessagePolling.Offset, 1ul), (skip.Kind, skip.Value));
        Assert.Equal([1ul, 2ul], resumed.Messages.Select(message => message.Header.Offset));
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

    /// <summary>A read with a fully zero context names no history, so a refused history reads again.</summary>
    [Fact]
    public async Task given_reads_with_a_fully_zero_context_when_the_history_is_refused_should_read_again()
    {
        using var cluster = new ReaderCluster(1) { Limit = 1000, ReadsUnstamped = true };
        using var client = await cluster.ConnectAsync();
        await using var reader = await Reader(client, CommitPolicy.Manual, 1, 2).BuildAsync(Token);
        await NextOfAsync(reader, 1);
        cluster.HistoryRefused.Add(1);
        var fetches = cluster.Fetches.Count;

        // Two refused reads of partition 1 show that the reader read again after the first.
        await UntilAsync(() => cluster.Fetches.Skip(fetches)
            .Count(fetch => fetch.Request.Targets.Any(target => target.PartitionId == 1)) >= 2);
        cluster.HistoryRefused.Clear();
        var resumed = await NextOfAsync(reader, 1);

        Assert.Equal(1u, resumed.PartitionId);
    }

    /// <summary>
    ///     A Next position with a fully zero context names no offset, as one without a context, so under OnRead it
    ///     first finds its start, from the builder or from a seek.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task given_on_read_with_a_next_position_of_a_fully_zero_context_when_reading_should_first_find_its_start(
        bool seeks)
    {
        using var cluster = new ReaderCluster(1) { Limit = 10 };
        cluster.Stored[1] = 4;
        using var client = await cluster.ConnectAsync();
        var zero = PollingStrategy.Next().WithContext(default);
        await using var reader = await Reader(client, CommitPolicy.OnRead, 1)
            .WithStart(seeks ? PollingStrategy.Next().WithContext(ReaderCluster.Served) : zero).BuildAsync(Token);
        if (seeks)
        {
            await ReadAsync(reader, 1);
            await reader.SeekAsync(1, zero, Token);
        }

        var batch = (await ReadAsync(reader, 1))[0];

        Assert.Equal(1, cluster.Probes);
        Assert.Equal(seeks ? 7ul : 5ul, batch.Messages[0].Header.Offset);
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

    /// <summary>
    ///     The batch at offset 2 does not decode, so the partition reads it one message at a time. A refusal as not
    ///     owned alone is not a leave, so the partition keeps its position and polls 2 alone again instead of its Last
    ///     start.
    /// </summary>
    [Fact]
    public async Task given_a_last_start_when_a_read_alone_is_refused_as_not_owned_should_poll_its_position_alone_again()
    {
        using var cluster = new ReaderCluster(1) { Limit = 8, RecoveryNotOwned = true };
        cluster.Undecodable.Add(2);
        using var client = await cluster.ConnectAsync();
        await using var reader = await Reader(client, CommitPolicy.Manual, 1).WithStart(PollingStrategy.Last())
            .BuildAsync(Token);
        Assert.Equal([6ul, 7ul], (await ReadAsync(reader, 1))[0].Messages.Select(message => message.Header.Offset));
        var starts = cluster.Polls.Count(poll => poll.Kind == MessagePolling.Last);

        await reader.SeekAsync(1, PollingStrategy.Offset(2), Token);
        await UntilAsync(() => cluster.Polls.Count(poll => poll is { Kind: MessagePolling.Offset, Value: 2, Count: 1 }) == 2);

        Assert.Equal(starts, cluster.Polls.Count(poll => poll.Kind == MessagePolling.Last));
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

        await reader.CloseAsync(Token);

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
        await reader.CloseAsync(Token);

        Assert.Equal(0ul, cluster.Stored[1]);
    }

    /// <summary>
    ///     A store of a counted position lands with an unknown outcome: a store of the policy, or the store of the
    ///     close. The reader releases it in the client, so the next store of the partition on that client is not
    ///     blocked.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task given_auto_commit_when_a_store_has_an_unknown_outcome_should_free_the_partition_for_the_next_store(
        bool closing)
    {
        using var cluster = new ReaderCluster(1) { Limit = 6, LoseNextStoreReply = true };
        using var client = await cluster.ConnectAsync();
        var policy = CommitPolicy.Auto(closing
            ? new AutoCommit { Interval = TimeSpan.FromHours(1), After = CommitAfter.Delivery }
            : new AutoCommit { Every = CommitEvery.Batch, After = CommitAfter.Delivery });
        var reader = await Reader(client, policy, 1).BuildAsync(Token);
        await ReadAsync(reader, 1);
        if (closing)
        {
            await Assert.ThrowsAsync<VsrRequestOutcomeUnknownException>(() => reader.CloseAsync(Token).AsTask());
            reader = await Reader(client, policy, 1).BuildAsync(Token);
        }
        else
        {
            await UntilAsync(() => !cluster.LoseNextStoreReply);
        }

        await ReadAsync(reader, 1);
        await reader.CloseAsync(Token);

        Assert.Equal(3ul, cluster.Stored[1]);
    }

    /// <summary>
    ///     The store of a manual commit lands with an unknown outcome, so the commit fails with that error. The reader
    ///     releases the store in the client, so a retry of the commit reaches the cluster and stores again.
    /// </summary>
    [Fact]
    public async Task given_manual_commit_when_its_store_has_an_unknown_outcome_should_fail_it_and_let_a_retry_store()
    {
        using var cluster = new ReaderCluster(1) { Limit = 4, LoseNextStoreReply = true };
        using var client = await cluster.ConnectAsync();
        await using var reader = await Reader(client, CommitPolicy.Manual, 1).BuildAsync(Token);
        var checkpoint = (await ReadAsync(reader, 1))[0].Checkpoint!;

        await Assert.ThrowsAsync<VsrRequestOutcomeUnknownException>(() => reader.CommitAsync(checkpoint, Token));
        Assert.True(cluster.Stored.TryRemove(1, out _));
        await reader.CommitAsync(checkpoint, Token);

        Assert.Equal(checkpoint.Offset, cluster.Stored[1]);
    }

    /// <summary>
    ///     A rebalance takes partition 1 away, and the store of its counted position lands with an unknown outcome. The
    ///     reader releases it in the client, so the close stores the partition after the rebalance gives it back.
    /// </summary>
    [Fact]
    public async Task given_auto_commit_when_the_store_of_a_partition_that_left_has_an_unknown_outcome_should_free_the_partition()
    {
        using var cluster = new ReaderCluster(1) { Limit = 6, Assignment = [1] };
        using var client = await cluster.ConnectAsync();
        var policy = CommitPolicy.Auto(new AutoCommit { Interval = TimeSpan.FromHours(1), After = CommitAfter.Delivery });
        var reader = await client.Reader(Stream, Topic, Consumer.Group("readers"))
            .WithOptions(new PollOptions { MaxWait = TimeSpan.FromMilliseconds(100), Commit = policy })
            .WithRetryInterval(RetryInterval).BuildAsync(Token);
        await ReadAsync(reader, 1);
        cluster.LoseNextStoreReply = true;
        cluster.Assignment = [];
        await UntilAsync(() => !cluster.LoseNextStoreReply, ConsumerGroupClientState.AssignmentRefreshMs * 2);
        cluster.Assignment = [1];
        await UntilAsync(() => reader.Partitions.Count == 1, ConsumerGroupClientState.AssignmentRefreshMs * 2);
        await ReadAsync(reader, 1);

        await reader.CloseAsync(Token);

        Assert.Equal(3ul, cluster.Stored[1]);
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
        await reader.CloseAsync(Token);

        Assert.Equal([1u, 2u], delivered.Select(batch => batch.PartitionId).Order());
        Assert.Equal([1u, 2u], cluster.Fetches.SelectMany(fetch => fetch.Request.Targets)
            .Select(target => target.PartitionId).Distinct().Order());
        Assert.Equal((1, 1), (cluster.Joins, cluster.Leaves));
    }

    /// <summary>
    ///     The coordinator hands a joining member its partitions one at a time, so the first assignment holds only
    ///     partition 1. The reader asks again within its refresh interval and reads partition 3 well before the client
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
            await reader.CloseAsync(Token);
        }
    }

    /// <summary>
    ///     A join moves ownership only after the partition logs persist the new owner, so the first sync after the
    ///     join lists no partition and the next one lists partition 1. The member syncs again soon after the join.
    ///     Nothing else wakes the reader: the application waits for no batch before the fetch starts.
    /// </summary>
    [Fact]
    public async Task given_a_join_whose_partition_moves_after_the_first_sync_when_reading_should_fetch_it_before_the_refresh_interval()
    {
        var limit = PartitionReader.GroupRefreshInterval / 2;
        using var cluster = new ReaderCluster(1) { AssignmentAfterSync = [1] };
        using var client = await cluster.ConnectAsync();
        var started = Stopwatch.GetTimestamp();
        var reader = await client.Reader(Stream, Topic, Consumer.Group("readers"))
            .WithOptions(new PollOptions { MaxWait = TimeSpan.FromMilliseconds(100) })
            .WithRetryInterval(RetryInterval).BuildAsync(Token);
        try
        {
            await UntilAsync(() => cluster.Fetches.Any(fetch => fetch.Request.Targets.Any(target => target.PartitionId == 1)));
            var fetched = Stopwatch.GetElapsedTime(started);

            Assert.True(fetched < limit, $"The member fetched partition 1 after {fetched.TotalMilliseconds} ms.");
            Assert.Equal(1u, (await ReadAsync(reader, 1))[0].PartitionId);
        }
        finally
        {
            await reader.CloseAsync(Token);
        }
    }

    /// <summary>
    ///     The partitions of the member never change, while hand-overs between other members move the group to a new
    ///     generation on each sync. Once the pauses after the join have doubled up to the refresh interval, the member
    ///     syncs once per interval.
    /// </summary>
    [Fact]
    public async Task given_an_assignment_that_never_changes_when_the_group_generation_moves_should_sync_once_per_refresh_interval()
    {
        const int Intervals = 3;
        var settled = TimeSpan.FromSeconds(2);
        using var cluster = new ReaderCluster(1) { Assignment = [1], RebalancesEachSync = true };
        using var client = await cluster.ConnectAsync();
        var reader = await client.Reader(Stream, Topic, Consumer.Group("readers"))
            .WithOptions(new PollOptions { MaxWait = TimeSpan.FromMilliseconds(100) })
            .WithRetryInterval(RetryInterval).BuildAsync(Token);
        try
        {
            await Task.Delay(settled, Token);
            var before = cluster.Syncs;

            await Task.Delay(GroupRefreshInterval * Intervals, Token);

            // Each edge of the window may hold a sync, and the heartbeat of the client syncs the group too.
            Assert.InRange(cluster.Syncs - before, Intervals - 1, Intervals + 2);
        }
        finally
        {
            await reader.CloseAsync(Token);
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
        await reader.CloseAsync(Token);

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
            await reader.CloseAsync(Token);
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

        await reader.CloseAsync(Token);

        Assert.True(readerErrors > 0);
        Assert.InRange(cluster.RouteLookups, 1, (int)(window / RetryInterval) + 2);
    }

    /// <summary>
    ///     Partition 2 joins the assignment while every lookup of its route is refused, as lasting or as transient,
    ///     which the client retries until its deadline. No lookup groups it, and partition 1 keeps the group of the
    ///     last lookup.
    /// </summary>
    [Theory]
    [InlineData(VsrError.UNAUTHORIZED)]
    [InlineData(VsrError.TRANSIENT_NOT_ACCEPTED)]
    public async Task given_a_new_partition_whose_route_lookup_is_refused_when_reading_should_keep_reading_the_grouped_one(
        int refusal)
    {
        const uint Grouped = 1;
        const uint Unrouted = 2;
        const int ReadsAfterJoin = 3;
        using var cluster = new ReaderCluster(1)
        {
            Assignment = [Grouped],
            Limit = (ulong)(ReadsAfterJoin + 1) * GOLDEN_BATCH_MESSAGES
        };
        using var client = await cluster.ConnectAsync();
        var reader = await client.Reader(Stream, Topic, Consumer.Group("readers"))
            .WithStart(PollingStrategy.Offset(0))
            .WithOptions(new PollOptions { MaxWait = TimeSpan.FromMilliseconds(100), Commit = CommitPolicy.OnRead })
            .WithRetryInterval(RetryInterval).BuildAsync(Token);
        try
        {
            await NextOfAsync(reader, Grouped);
            cluster.RefusedRoutes[Unrouted] = refusal;
            cluster.Assignment = [Grouped, Unrouted];
            await UntilAsync(() => reader.Partitions.Contains(Unrouted));

            // With an on read policy each batch comes from a read that started while NextAsync waited.
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(Token);
            timeout.CancelAfter(PromptLimit);
            var reads = 0;
            while (reads < ReadsAfterJoin)
            {
                try
                {
                    if ((await reader.NextAsync(timeout.Token)).PartitionId == Grouped)
                    {
                        reads++;
                    }
                }
                catch (ReadException error) when (error.PartitionId != Grouped)
                {
                    // The refused lookups report for the whole reader, and partition 2 for its own reads.
                }
            }

            await UntilAsync(() => cluster.RefusedRouteLookups > 0);
        }
        finally
        {
            await reader.CloseAsync(Token);
        }
    }

    [Fact]
    public async Task given_partitions_of_one_primary_when_a_fetch_reaches_its_count_should_give_each_partition_its_turn()
    {
        uint[] partitions = [1, 3, 5];
        using var cluster = new ReaderCluster(2) { Limit = 100, CountLimited = true, HoldFetches = true };
        using var client = await cluster.ConnectAsync();
        var reader = await client.Reader(Stream, Topic, Consumer.New(4))
            .WithPartitions(partitions)
            .WithOptions(new PollOptions { Count = 2, MaxWait = TimeSpan.FromMilliseconds(100) })
            .WithRetryInterval(RetryInterval).BuildAsync(Token);
        try
        {
            foreach (var partition in partitions)
            {
                await UntilAsync(() => cluster.Held.Sum(fetch => fetch.Targets.Length) == partitions.Length);
                Assert.Contains(cluster.Held, fetch => fetch.Targets[0].PartitionId == partition);
                var reads = cluster.HeldFetches;
                await cluster.ReleaseHeldAsync(reads);

                var delivered = await ReadAsync(reader, reads);
                Assert.Contains(delivered, batch => batch.PartitionId == partition);
            }
        }
        finally
        {
            cluster.HoldFetches = false;
            await cluster.ReleaseHeldAsync();
            await reader.CloseAsync(Token);
        }
    }

    /// <summary>
    ///     Every partition of the primary ended its last read before its head, so each one reads in a fetch of its own,
    ///     with the full limits, and the reader keeps as many reads in flight as separate polls would.
    /// </summary>
    [Fact]
    public async Task given_backlogged_partitions_of_one_primary_when_slots_are_free_should_read_each_in_its_own_fetch()
    {
        const int Partitions = 32;
        // The batches of the first fetch wait in the queue and take one batch of the budget.
        const uint Batches = Partitions + 1;
        var options = new PollOptions();
        using var cluster = new ReaderCluster(1) { Limit = 100, ReportsHead = true, HoldFetches = true };
        using var client = await cluster.ConnectAsync();
        var reader = await Reader(client, CommitPolicy.Manual,
                [.. Enumerable.Range(0, Partitions).Select(partition => (uint)partition)])
            .WithPrefetch(options.MaxBytes * Batches, options.Count * Batches).BuildAsync(Token);
        try
        {
            await UntilAsync(() => cluster.HeldFetches == 1);
            await cluster.ReleaseHeldAsync();

            await UntilAsync(() => cluster.Held.Sum(fetch => fetch.Targets.Length) == Partitions);

            Assert.All(cluster.Held, fetch => Assert.Single(fetch.Targets));
            Assert.Equal(Partitions, cluster.HeldFetches);
        }
        finally
        {
            cluster.HoldFetches = false;
            await cluster.ReleaseHeldAsync();
            await reader.CloseAsync(Token);
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
            await reader.CloseAsync(Token);
        }
    }

    /// <summary>
    ///     Every partition waits for its first message, so each one polls alone, and the polls leave one request for
    ///     the group of their primary. A poll that ends empty gives its request to the partition after the last polled
    ///     one.
    /// </summary>
    [Fact]
    public async Task given_more_partitions_polling_alone_than_requests_when_a_poll_ends_empty_should_poll_the_next_partition()
    {
        const int Polls = ReaderRequests - 1;
        using var cluster = new ReaderCluster(1) { Limit = 0, HoldPolls = true };
        using var client = await cluster.ConnectAsync();
        var reader = await Reader(client, CommitPolicy.OnRead, Partitions(ReaderRequests + 2)).BuildAsync(Token);
        try
        {
            // A committing reader reads only while the application waits, and no batch ever arrives here.
            _ = reader.NextAsync(Token).AsTask();
            await UntilAsync(() => cluster.HeldPolls.Count >= Polls);
            await Task.Delay(SettleWindow, Token);
            Assert.Equal(Partitions(Polls), cluster.HeldPolls.Order());
            var released = cluster.HeldPolls[0];

            await cluster.ReleaseHeldPollsAsync(1);

            await UntilAsync(() => cluster.HeldPolls.Contains((uint)Polls));
            Assert.Equal(Partitions(Polls + 1).Where(partition => partition != released), cluster.HeldPolls.Order());
        }
        finally
        {
            cluster.HoldPolls = false;
            await cluster.ReleaseHeldPollsAsync();
            await reader.CloseAsync(Token);
        }
    }

    /// <summary>
    ///     Each partition has a primary of its own, so each fetch takes one request. The primary that the requests left
    ///     out fetches next, before a primary that fetched already.
    /// </summary>
    [Fact]
    public async Task given_more_primaries_than_requests_when_a_fetch_ends_should_fetch_the_primary_left_out()
    {
        const int Primaries = ReaderRequests + 1;
        using var cluster = new ReaderCluster(Primaries) { HoldFetches = true };
        using var client = await cluster.ConnectAsync();
        var reader = await Reader(client, CommitPolicy.Manual, Partitions(Primaries))
            .WithStart(PollingStrategy.Offset(0)).BuildAsync(Token);
        try
        {
            await UntilAsync(() => cluster.HeldFetches >= ReaderRequests);
            await Task.Delay(SettleWindow, Token);
            Assert.Equal(Partitions(ReaderRequests), HeldTargets(cluster));
            var released = cluster.Held[0].Targets[0].PartitionId;

            await cluster.ReleaseHeldAsync(1);

            await UntilAsync(() => HeldTargets(cluster).Contains((uint)ReaderRequests));
            Assert.Equal(Partitions(Primaries).Where(partition => partition != released), HeldTargets(cluster));
        }
        finally
        {
            cluster.HoldFetches = false;
            await cluster.ReleaseHeldAsync();
            await reader.CloseAsync(Token);
        }
    }

    /// <summary>
    ///     A start by timestamp makes each partition poll alone, and the polls leave one request for each of the three
    ///     primaries. A partition that its poll moved to an offset fetches at once, and the next partition that waits
    ///     polls in its place.
    /// </summary>
    [Fact]
    public async Task given_partitions_polling_alone_and_three_primaries_when_reading_should_leave_a_request_for_each_primary()
    {
        const int Primaries = 3;
        const int Polls = ReaderRequests - Primaries;
        using var cluster = new ReaderCluster(Primaries) { HoldPolls = true, HoldFetches = true };
        using var client = await cluster.ConnectAsync();
        var reader = await Reader(client, CommitPolicy.Manual, Partitions(ReaderRequests + Primaries))
            .WithStart(PollingStrategy.Timestamp(0)).BuildAsync(Token);
        try
        {
            await UntilAsync(() => cluster.HeldPolls.Count >= Polls);
            await Task.Delay(SettleWindow, Token);
            Assert.Equal(Partitions(Polls), cluster.HeldPolls.Order());
            List<uint> released = [];
            for (var fetches = 1; fetches <= Primaries; fetches++)
            {
                released.Add(cluster.HeldPolls[0]);
                await cluster.ReleaseHeldPollsAsync(1);
                await UntilAsync(() => cluster.HeldFetches == fetches);
            }

            await Task.Delay(SettleWindow, Token);
            Assert.Equal(released.Order(), HeldTargets(cluster));
            Assert.Equal(Partitions(ReaderRequests).Except(released), cluster.HeldPolls.Order());
        }
        finally
        {
            cluster.HoldPolls = false;
            cluster.HoldFetches = false;
            await cluster.ReleaseHeldPollsAsync();
            await cluster.ReleaseHeldAsync();
            await reader.CloseAsync(Token);
        }
    }

    /// <summary>
    ///     With a commit on read each partition first polls alone for its start, and the polls leave one request for
    ///     each of the three primaries. A seek moves a partition that waits to poll to an offset, and its primary
    ///     fetches it at once. Once the application takes the batch and asks again, the primary fetches again at once,
    ///     while no poll has ended.
    /// </summary>
    [Fact]
    public async Task given_partitions_polling_alone_and_three_primaries_when_a_seek_moves_a_partition_to_an_offset_should_fetch_it_at_once()
    {
        const int Primaries = 3;
        const int Polls = ReaderRequests - Primaries;
        const uint Sought = ReaderRequests + Primaries - 1;
        using var cluster = new ReaderCluster(Primaries) { HoldPolls = true };
        using var client = await cluster.ConnectAsync();
        var reader = await Reader(client, CommitPolicy.OnRead, Partitions(ReaderRequests + Primaries)).BuildAsync(Token);
        try
        {
            var first = reader.NextAsync(Token).AsTask();
            await UntilAsync(() => cluster.HeldPolls.Count >= Polls);
            await reader.SeekAsync(Sought, PollingStrategy.Offset(0), Token);
            var batch = await first.WaitAsync(PromptLimit, Token);

            _ = reader.NextAsync(Token).AsTask();

            await UntilAsync(() =>
                cluster.Fetches.Count(fetch => fetch.Request.Targets.Any(target => target.PartitionId == Sought)) >= 2);
            Assert.Equal((Sought, 0ul), (batch.PartitionId, batch.Messages[0].Header.Offset));
            Assert.Equal(Polls, cluster.HeldPolls.Count);
        }
        finally
        {
            cluster.HoldPolls = false;
            await cluster.ReleaseHeldPollsAsync();
            await reader.CloseAsync(Token);
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
        await reader.CloseAsync(Token);

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

    /// <summary>
    ///     The owner generation that each fetch of a single partition named, from fetch <paramref name="from" /> on.
    /// </summary>
    private static ulong[] FetchOwners(ReaderCluster cluster, int from)
    {
        return [.. cluster.Fetches.Skip(from).Select(fetch => Assert.Single(fetch.Request.Targets).OwnerGeneration)];
    }

    private static uint[] Partitions(int count)
    {
        return [.. Enumerable.Range(0, count).Select(partition => (uint)partition)];
    }

    private static IEnumerable<uint> HeldTargets(ReaderCluster cluster)
    {
        return cluster.Held.SelectMany(fetch => fetch.Targets).Select(target => target.PartitionId).Order();
    }

    private static async Task ReleaseSeekRequestsAsync(ReaderCluster cluster)
    {
        cluster.HoldStores = cluster.HoldLookups = cluster.HoldPolls = false;
        await cluster.ReleaseHeldStoresAsync();
        await cluster.ReleaseHeldLookupsAsync();
        await cluster.ReleaseHeldPollsAsync();
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

    /// <summary>
    ///     Signs the client in under a new session. The reader joins again, and the cluster installs the owner again
    ///     with a new generation at the metadata op of the join, past the op of every context it served, as the server
    ///     does. The reply of the join shows that op to the client.
    /// </summary>
    private static async Task SignInAgainAsync(ReaderCluster cluster, TcpMessageStream client)
    {
        cluster.OwnerGeneration = ReaderCluster.Served.OwnerGeneration + 1;
        cluster.InstallOp = ReaderCluster.Served.MetadataOp + 1;
        await client.LoginUserAsync("user", "secret", Token);
        await UntilAsync(() => cluster.Joins == 2
                               && ((IConsumerGroupAssignmentProvider)client).MetadataWatermark >= cluster.InstallOp,
            ConsumerGroupClientState.AssignmentRefreshMs * 2);
    }

    /// <summary>The next batch, past the errors of the whole reader that a lost session leaves.</summary>
    private static async Task<PolledMessages> NextBatchAsync(PartitionReader reader)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(Token);
        timeout.CancelAfter(LostReplyLimit);
        while (true)
        {
            try
            {
                return await reader.NextAsync(timeout.Token);
            }
            catch (ReadException error) when (error.PartitionId is null)
            {
            }
        }
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

    /// <summary>The next item of the reader, which must report a lost range that the history no longer holds.</summary>
    private static async Task<ReadException> GoneRangeAsync(PartitionReader reader)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(Token);
        timeout.CancelAfter(PromptLimit);
        var error = await Assert.ThrowsAsync<ReadException>(() => reader.NextAsync(timeout.Token).AsTask());
        var lost = Assert.IsAssignableFrom<IggyInvalidStatusCodeException>(error.InnerException);
        Assert.Equal(VsrError.FETCH_PROGRESS_UNCERTAIN, lost.StatusCode);
        Assert.Contains("no longer holds", lost.Message);
        return error;
    }

    /// <summary>A reply of partition 1 with the two messages from <paramref name="from" />.</summary>
    private static PolledMessages Served(ulong from)
    {
        return new PolledMessages
        {
            PartitionId = 1,
            CurrentOffset = 9,
            Context = ReaderCluster.Served,
            Messages = [.. new[] { from, from + 1 }.Select(offset =>
                new MessageResponse { Header = new MessageHeader { Offset = offset }, Payload = [1] })]
        };
    }

    /// <summary>
    ///     Lets the held first fetches of the reader commit while no call waits for them, so the batch of each
    ///     partition after the stored offset 3 stays queued and the server stores 5.
    /// </summary>
    private static async Task QueueFirstBatchAsync(ReaderCluster cluster, PartitionReader reader)
    {
        using (var waiting = CancellationTokenSource.CreateLinkedTokenSource(Token))
        {
            var next = reader.NextAsync(waiting.Token).AsTask();
            await UntilAsync(() => cluster.Held.Sum(fetch => fetch.Targets.Length) == cluster.Assignment.Length);
            await waiting.CancelAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => next);
        }

        cluster.HoldFetches = false;
        await cluster.ReleaseHeldAsync();
        await UntilAsync(() => cluster.Assignment.All(partition => cluster.Stored[partition] == 5));
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

    /// <summary>Copies each message as is. A decryption cancels <see cref="Cancels" /> once it is set.</summary>
    private sealed class CancelingEncryptor : IMessageEncryptor
    {
        internal CancellationTokenSource? Cancels { get; set; }

        public int GetMaxEncryptedLength(int plaintextLength) => plaintextLength;

        public int GetMaxDecryptedLength(int encryptedLength) => encryptedLength;

        public int Encrypt(ReadOnlySpan<byte> plaintext, Span<byte> destination)
        {
            plaintext.CopyTo(destination);
            return plaintext.Length;
        }

        public int Decrypt(ReadOnlySpan<byte> encryptedData, Span<byte> destination)
        {
            Cancels?.Cancel();
            encryptedData.CopyTo(destination);
            return encryptedData.Length;
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
        private ulong _firstKept;
        private int _recoveryReads;
        private ulong? _stored;

        internal ulong End { get; set; }

        internal bool LoseNextCommittingRead { get; set; }

        /// <summary>Offsets whose message does not decode, so a reply that holds one fails to decode.</summary>
        internal HashSet<ulong> Malformed { get; } = [];

        /// <summary>A read by offset without a commit finds nothing.</summary>
        internal bool RecoveryEmpty { get; init; }

        /// <summary>
        ///     From the first read by offset without a commit, the log holds only the messages from this offset, as
        ///     after retention, and a read below it starts at it.
        /// </summary>
        internal ulong RecoveryRetainsFrom { get; init; }

        internal int RecoveryReads => Volatile.Read(ref _recoveryReads);

        /// <summary>A reader of partition 1 that reads 4 messages at a time and commits on read.</summary>
        internal async Task<PartitionReader> ReaderAsync()
        {
            return await Client().Reader(Stream, Topic, Consumer.New(4)).WithPartitions(1)
                .WithOptions(new PollOptions
                {
                    Count = 4,
                    MaxWait = TimeSpan.FromMilliseconds(100),
                    Commit = CommitPolicy.OnRead
                })
                .WithRetryInterval(RetryInterval).BuildAsync(Token);
        }

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
                var commits = options.Commit.Mode == CommitMode.OnRead;
                var recovers = start.Kind == MessagePolling.Offset && !commits;
                if (recovers && Interlocked.Increment(ref _recoveryReads) == 1)
                {
                    _firstKept = RecoveryRetainsFrom;
                }

                var from = Math.Max(_firstKept, start.Kind == MessagePolling.Next ? _stored + 1 ?? 0 : start.Value);
                var until = recovers && RecoveryEmpty ? from : Math.Min(End, from + options.Count);

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
