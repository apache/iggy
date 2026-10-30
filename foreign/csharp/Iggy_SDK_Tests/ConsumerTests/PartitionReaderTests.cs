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

        cluster.Stored[1] = 3;
        var delivered = await ReadAsync(reader, 3);

        Assert.Equal([2ul, 3ul, 4ul, 5ul, 6ul, 7ul],
            delivered.SelectMany(batch => batch.Messages).Select(message => message.Header.Offset));
        Assert.Equal([(MessagePolling.Offset, 2ul, false), (MessagePolling.Offset, 4ul, false)],
            cluster.Polls.Where(poll => poll.Kind == MessagePolling.Offset)
                .Select(poll => (poll.Kind, poll.Value, poll.AutoCommit)));
        Assert.Equal(7ul, cluster.Stored[1]);
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
            cluster.Stored[1] = 3;
            cluster.AssignmentAfterRead = [];
            cluster.RecoveryNotOwned = true;
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(Token);
            timeout.CancelAfter(TimeSpan.FromSeconds(5));
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

    private static async Task<List<PolledMessages>> ReadAsync(PartitionReader reader, int batches)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(Token);
        timeout.CancelAfter(PromptLimit);
        List<PolledMessages> delivered = [];
        while (delivered.Count < batches)
        {
            delivered.Add(await reader.NextAsync(timeout.Token));
        }

        return delivered;
    }

    private static async Task<PolledMessages> NextOfAsync(PartitionReader reader, uint partition)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(Token);
        timeout.CancelAfter(PromptLimit);
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
}
