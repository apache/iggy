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

using System.Runtime.CompilerServices;
using Apache.Iggy.Contracts;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Kinds;
using Apache.Iggy.Mappers;
using Apache.Iggy.Vsr;
using Microsoft.Extensions.Logging;

namespace Apache.Iggy.Consumers;

/// <summary>
///     Reads the partitions of a topic in batches, with routing, read ahead and the position of each partition
///     inside. Build one with <see cref="Extensions.IggyClientExtension.Reader" />.
/// </summary>
/// <remarks>
///     <list type="bullet">
///         <item>
///             A standalone consumer reads the partitions of its builder. A consumer group reads the assignment of its
///             member: the reader joins the group, joins again after a session loss and leaves it in
///             <see cref="CloseAsync" />.
///         </item>
///         <item>
///             Over TCP the reader reads the partitions of one primary in one request, and finds a moved primary by
///             itself. Over HTTP it reads each partition with its own poll, and a consumer group is not available.
///         </item>
///         <item>
///             <see cref="NextAsync" /> returns the next batch of one partition, in offset order within one generation
///             of the partition: from its first read, from each <see cref="SeekAsync" />, and from each new assignment
///             to this member.
///         </item>
///         <item>
///             The <see cref="CommitPolicy" /> of the options decides who stores offsets. With
///             <see cref="CommitMode.Manual" />, <see cref="CommitAsync" /> stores the offset of a checkpoint. With
///             <see cref="CommitMode.OnRead" />, the server stores the offset of each read, the reader reads only
///             while the application waits, and <see cref="CloseAsync" /> moves the offset back to the last delivered
///             message. With <see cref="CommitMode.Auto" />, the reader counts each batch as its
///             <see cref="CommitAfter" /> says, or each <see cref="Mark" />, and stores the latest counted position at
///             the interval or the event of the policy and in <see cref="CloseAsync" />.
///         </item>
///         <item>
///             The reader stays usable after a <see cref="ReadException" /> and retries by itself. Status 87
///             (HistoryUnavailable) stops its partition until a <see cref="SeekAsync" /> when the position came from an
///             earlier read of a history that is gone. A refusal that a quick retry does not change, such as status 41
///             (Unauthorized), 3007 (PartitionNotFound) or 5009 (ConsumerGroupPartitionNotOwned), is retried after a
///             wait that doubles each time, and after six in a row it stops its partition until a
///             <see cref="SeekAsync" /> with one error. Status 5009 also drops the position of its partition, which
///             reads again from the start of the reader. After a read whose outcome is unknown the partition waits the
///             retry interval. With <see cref="CommitMode.OnRead" />, a read whose reply was lost is repaired: the next
///             read finds the messages that the lost read stored and reads them again without a commit before it goes
///             on. A read that starts past its position while no read was lost delivers from there.
///         </item>
///         <item>One caller at a time calls <see cref="NextAsync" />. The other members are safe to call concurrently.</item>
///     </list>
/// </remarks>
public sealed class PartitionReader : IAsyncDisposable
{
    private const int MaxLastingRefusals = 6;
    private const int MaxFetchSlots = 16;
    private const int ConsumerOffsetNotFound = 3021;
    private static readonly TimeSpan GroupRefreshInterval = TimeSpan.FromSeconds(1);
    private static long _readers;

    private readonly IConsumerGroupAssignmentProvider? _assignment;
    private readonly IIggyClient _client;
    private readonly IPartitionFetchClient? _fetcher;
    private readonly object _gate = new();
    private readonly long _id = Interlocked.Increment(ref _readers);
    private readonly CancellationTokenSource _lifetime = new();
    private readonly ILogger _logger;
    private readonly LinkedList<Entry> _queue = new();
    private readonly HashSet<Task> _reads = [];
    private readonly ReaderSettings _settings;
    private readonly SortedDictionary<uint, PartitionState> _states = [];
    private readonly Dictionary<uint, ulong> _sought = [];
    private readonly SemaphoreSlim _storeSignal = new(0, 1);
    private readonly SemaphoreSlim _wake = new(0, 1);
    private readonly Task _committer;
    private readonly Task _worker;
    private TaskCompletionSource _arrival = NewArrival();
    private bool _closed;
    private Task? _closing;
    private bool _demand;
    private int _fetching;
    private readonly List<Task> _seeks = [];
    private ulong _claims;
    private ulong _generations;
    private ulong _serves;
    private ulong _joinedSession;
    private long _queuedBytes;
    private long _queuedMessages;
    private bool _resync;
    private long? _syncedAt;
    private (Checkpoint Checkpoint, uint Messages)? _uncounted;

    internal PartitionReader(IIggyClient client, IConsumerGroupAssignmentProvider? assignment,
        ReaderSettings settings, ILogger logger)
    {
        _client = client;
        _assignment = assignment;
        _fetcher = client as IPartitionFetchClient;
        _settings = settings;
        _logger = logger;
        _joinedSession = (client as ISessionGenerationProvider)?.SessionGeneration ?? 0;
        foreach (var partition in settings.Partitions)
        {
            _states[partition] = new PartitionState(settings.Start, ++_generations);
        }

        _worker = Task.Run(RunAsync);
        _committer = AutoSettings is { } auto ? Task.Run(() => CommitLoopAsync(auto)) : Task.CompletedTask;
    }

    /// <summary>
    ///     The partitions that the reader reads: those of a standalone consumer, or the current assignment of a group
    ///     member. A group reader holds no partition until the first refresh of its assignment.
    /// </summary>
    public IReadOnlyList<uint> Partitions
    {
        get
        {
            lock (_gate)
            {
                return [.. _states.Keys];
            }
        }
    }

    private bool OnRead => _settings.Options.Commit.Mode == CommitMode.OnRead;

    private AutoCommit? AutoSettings => _settings.Options.Commit.Settings;

    /// <summary>Closes the reader as <see cref="CloseAsync" /> does, and logs the error of the close instead of throwing it.</summary>
    public async ValueTask DisposeAsync()
    {
        try
        {
            await CloseAsync();
        }
        catch (Exception error)
        {
            _logger.LogWarning(error, "Closing the partition reader failed.");
        }
    }

    /// <summary>
    ///     The next batch of one partition. Never empty, and it waits until a batch arrives. A canceled call leaves
    ///     the batches in the reader.
    /// </summary>
    /// <exception cref="ReadException">An error of a read. The reader stays usable.</exception>
    /// <exception cref="ObjectDisposedException">The reader is closed.</exception>
    public async ValueTask<PolledMessages> NextAsync(CancellationToken token = default)
    {
        CountUncounted();
        while (true)
        {
            Task arrival;
            lock (_gate)
            {
                ObjectDisposedException.ThrowIf(_closed, this);
                while (_queue.First is { } node)
                {
                    _queue.RemoveFirst();
                    var entry = node.Value;
                    Release(entry);
                    if (!IsCurrent(entry))
                    {
                        continue;
                    }

                    _demand = false;
                    if (entry.Error is { } error)
                    {
                        throw error;
                    }

                    return Deliver(entry.Batch!);
                }

                _demand = true;
                arrival = _arrival.Task;
            }

            Wake();
            await arrival.WaitAsync(token);
        }
    }

    /// <summary>Every batch, until a read fails or <paramref name="token" /> is canceled.</summary>
    /// <exception cref="ReadException">The first error of a read.</exception>
    public async IAsyncEnumerable<PolledMessages> ReadAllAsync([EnumeratorCancellation] CancellationToken token = default)
    {
        while (true)
        {
            yield return await NextAsync(token);
        }
    }

    /// <summary>
    ///     Counts a checkpoint that this reader delivered, under <see cref="CommitMode.Auto" />. It stores nothing
    ///     now: the policy stores the latest counted position of the partition. With <see cref="CommitAfter.Marked" />
    ///     only marks count, so batches processed concurrently count once they are done.
    /// </summary>
    /// <exception cref="IggyInvalidStatusCodeException">
    ///     Status 3 without <see cref="CommitMode.Auto" />, and for a checkpoint that this reader did not deliver.
    /// </exception>
    public void Mark(Checkpoint checkpoint)
    {
        ArgumentNullException.ThrowIfNull(checkpoint);
        if (AutoSettings is null)
        {
            throw VsrError.Exception(VsrError.INVALID_COMMAND, "Only an automatic commit policy counts marks.");
        }

        ThrowIfForeign(checkpoint);
        Count(checkpoint, 1, false);
    }

    /// <summary>
    ///     Stores the offset of a checkpoint that this reader delivered. A checkpoint of an older generation of its
    ///     partition, or at or below the last commit of its generation, stores nothing.
    /// </summary>
    /// <exception cref="IggyInvalidStatusCodeException">
    ///     Status 3 with <see cref="CommitMode.OnRead" />, and for a checkpoint that this reader did not deliver. The
    ///     errors of the offset store.
    /// </exception>
    public async Task CommitAsync(Checkpoint checkpoint, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(checkpoint);
        if (OnRead)
        {
            throw VsrError.Exception(VsrError.INVALID_COMMAND, "The server stores the offsets of an on read policy.");
        }

        ThrowIfForeign(checkpoint);
        lock (_gate)
        {
            if (!_states.TryGetValue(checkpoint.PartitionId, out var state)
                || state.Generation != checkpoint.Stamp!.Value.Generation
                || state.Stored is { } stored && stored.Offset >= checkpoint.Offset)
            {
                return;
            }
        }

        await _client.StoreConsumerPositionAsync(_settings.Consumer, _settings.StreamId, _settings.TopicId,
            checkpoint, token);
        RecordStored(checkpoint);
    }

    /// <summary>
    ///     Sets the next read position of <paramref name="partitionId" /> and starts a new generation of it. It stores
    ///     the queued batches of the partition, ignores its late replies, and resumes a stopped partition. With
    ///     <see cref="CommitMode.OnRead" />, a seek to an offset above 0 first stores the offset before it on the
    ///     server, so a restart resumes at the sought offset, a refused store fails the seek and moves nothing, and the
    ///     seek then waits for the read of the partition in flight, whose batch it drops. Every other seek stores
    ///     nothing.
    /// </summary>
    /// <exception cref="IggyInvalidStatusCodeException">
    ///     Status 3 for a partition that the reader does not read, and for a continuation of another partition.
    /// </exception>
    /// <exception cref="ObjectDisposedException">The reader is closed.</exception>
    public async Task SeekAsync(uint partitionId, PollingStrategy start, CancellationToken token = default)
    {
        var seek = SeekOnceAsync(partitionId, start, token);
        lock (_gate)
        {
            _seeks.Add(seek);
        }

        try
        {
            await seek;
        }
        finally
        {
            lock (_gate)
            {
                _seeks.Remove(seek);
            }
        }
    }

    private async Task SeekOnceAsync(uint partitionId, PollingStrategy start, CancellationToken token)
    {
        start.ValidatePartition(partitionId);
        if (start.Kind == MessagePolling.First)
        {
            start = start with { Kind = MessagePolling.Offset, Value = 0 };
        }

        (ulong Offset, PartitionContext Context)? stored = null;
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_closed, this);
            var state = State(partitionId);
            if (OnRead && start.Kind == MessagePolling.Offset && start.Value > 0)
            {
                stored = (start.Value - 1, start.Context ?? state.Handed?.Context ?? default);
            }
        }

        if (stored is { } store)
        {
            await _client.StoreOffsetAsync(_settings.Consumer, _settings.StreamId, _settings.TopicId, store.Offset,
                partitionId, store.Context, token);
        }

        Task idle;
        bool raced;
        lock (_gate)
        {
            var state = State(partitionId);
            idle = state.InFlight && OnRead ? state.Idle.Task : Task.CompletedTask;
            raced = state.InFlight || _queue.Any(entry => entry.PartitionId == partitionId && entry.Batch is not null);
        }

        await idle.WaitAsync(token);

        lock (_gate)
        {
            var state = State(partitionId);
            state.Generation = ++_generations;
            _sought[partitionId] = state.Generation;
            state.Position = start with { PartitionId = partitionId };
            state.Stopped = null;
            state.Lasting = 0;
            state.RetryAt = 0;
            state.CatchUpUntil = null;
            state.CatchUpBatch = null;
            // A committing read that the server admitted after the store moved the offset on, so the close puts it back.
            state.PendingStart = raced && stored is not null ? state.Position : null;
            state.Lost = false;
            state.AloneUntil = null;
            state.Counted = null;
            state.CountedSinceStore = 0;
            state.Stored = null;
            state.Delivered = stored;
            state.Handed = stored;
            state.FirstHanded = null;
            for (var node = _queue.First; node is not null;)
            {
                var next = node.Next;
                if (node.Value.PartitionId == partitionId)
                {
                    Release(node.Value);
                    _queue.Remove(node);
                }

                node = next;
            }
        }

        Wake();
    }

    /// <summary>
    ///     Stops the reader. It waits for the reads in flight. With <see cref="CommitMode.Manual" /> it commits
    ///     nothing. With <see cref="CommitMode.Auto" />, it counts the last batch under
    ///     <see cref="CommitAfter.Processing" /> and stores the latest counted position of each partition. With
    ///     <see cref="CommitMode.OnRead" />, it moves the offset of each partition with undelivered batches back to
    ///     its last delivered message. A partition that delivered nothing and started at Next gets the offset before
    ///     its first undelivered message, or no offset at offset 0. A group reader then leaves its group.
    /// </summary>
    /// <remarks>A later call waits for the first close and ends as it does.</remarks>
    /// <exception cref="Exception">The first failed store of the close.</exception>
    public async ValueTask CloseAsync()
    {
        TaskCompletionSource? closing = null;
        Task closed;
        lock (_gate)
        {
            if (_closing is null)
            {
                closing = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                _closing = closing.Task;
                _closed = true;
            }

            closed = _closing;
        }

        if (closing is not null)
        {
            try
            {
                await CloseOnceAsync();
                closing.SetResult();
            }
            catch (Exception error)
            {
                closing.SetException(error);
            }
        }

        await closed;
    }

    private async Task CloseOnceAsync()
    {
        CountUncounted();
        await _lifetime.CancelAsync();
        await _worker;
        Task[] reads;
        lock (_gate)
        {
            reads = [.. _reads];
        }

        await Task.WhenAll(reads);
        Task[] seeks;
        lock (_gate)
        {
            seeks = [.. _seeks];
        }

        // A seek whose store waited for a read still applies, so the rewind sees its position and not the dropped batch.
        foreach (var seek in seeks)
        {
            try
            {
                await seek;
            }
            catch (Exception)
            {
                // The seek reports its own failure.
            }
        }

        await _committer;
        Exception? failure = null;
        if (OnRead)
        {
            failure = await RewindAsync();
        }
        else if (AutoSettings is not null)
        {
            failure = await FlushCountedAsync();
        }

        if (_assignment is not null)
        {
            try
            {
                await _client.LeaveConsumerGroupAsync(_settings.StreamId, _settings.TopicId,
                    _settings.Consumer.ConsumerId);
            }
            catch (Exception error)
            {
                _logger.LogWarning(error, "The partition reader could not leave its consumer group.");
            }
        }

        lock (_gate)
        {
            _queue.Clear();
        }

        _lifetime.Dispose();
        if (failure is not null)
        {
            throw failure;
        }
    }

    private static TaskCompletionSource NewArrival()
    {
        return new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    /// <summary>
    ///     Records the last message that the owner of a reader built for a consumer handed over, so the close of an
    ///     on read policy moves the offset back to it.
    /// </summary>
    internal void Delivered(Checkpoint checkpoint)
    {
        lock (_gate)
        {
            if (_states.TryGetValue(checkpoint.PartitionId, out var state)
                && state.Generation == checkpoint.Stamp?.Generation)
            {
                state.Delivered = (checkpoint.Offset, state.Handed?.Context ?? default);
            }
        }
    }

    /// <summary>Counts <paramref name="messages" /> up to <paramref name="checkpoint" /> under an automatic policy.</summary>
    internal void CountDelivered(Checkpoint checkpoint, uint messages, bool endsBatch)
    {
        Count(checkpoint, messages, endsBatch);
    }

    private PolledMessages Deliver(PolledMessages batch)
    {
        var checkpoint = batch.Checkpoint!;
        if (_states.TryGetValue(batch.PartitionId, out var state) && state.Generation == checkpoint.Stamp?.Generation)
        {
            state.FirstHanded ??= batch.Messages[0].Header.Offset;
            state.Handed = (checkpoint.Offset, batch.Context);
            if (!_settings.CountsMessages)
            {
                state.Delivered = state.Handed;
            }
        }

        if (_settings.CountsMessages)
        {
            return batch;
        }

        switch (AutoSettings?.After)
        {
            case CommitAfter.Delivery:
                Count(checkpoint, (uint)batch.Messages.Count, true);
                break;
            case CommitAfter.Processing:
                _uncounted = (checkpoint, (uint)batch.Messages.Count);
                break;
        }

        return batch;
    }

    private void CountUncounted()
    {
        (Checkpoint Checkpoint, uint Messages)? uncounted;
        lock (_gate)
        {
            uncounted = _uncounted;
            _uncounted = null;
        }

        if (uncounted is { } batch)
        {
            Count(batch.Checkpoint, batch.Messages, true);
        }
    }

    private void Count(Checkpoint checkpoint, uint messages, bool endsBatch)
    {
        var due = false;
        lock (_gate)
        {
            if (!_states.TryGetValue(checkpoint.PartitionId, out var state)
                || state.Generation != checkpoint.Stamp?.Generation)
            {
                return;
            }

            if (state.Counted is null || state.Counted.Offset < checkpoint.Offset)
            {
                state.Counted = checkpoint;
            }

            state.CountedSinceStore += messages;
            due = AutoSettings?.Every is { } every && every.Kind switch
            {
                CommitEveryKind.Message => true,
                CommitEveryKind.Messages => state.CountedSinceStore >= every.Count,
                _ => endsBatch
            };
        }

        if (due)
        {
            Signal(_storeSignal);
        }
    }

    private void ThrowIfForeign(Checkpoint checkpoint)
    {
        if (checkpoint.Stamp?.Reader != _id)
        {
            throw VsrError.Exception(VsrError.INVALID_COMMAND, "The checkpoint was not delivered by this reader.");
        }
    }

    private void RecordStored(Checkpoint checkpoint)
    {
        lock (_gate)
        {
            if (_states.TryGetValue(checkpoint.PartitionId, out var state)
                && state.Generation == checkpoint.Stamp?.Generation
                && (state.Stored is null || state.Stored.Offset < checkpoint.Offset))
            {
                state.Stored = checkpoint;
                if (state.Counted is null || state.Counted.Offset <= checkpoint.Offset)
                {
                    state.CountedSinceStore = 0;
                }
            }
        }
    }

    private async Task CommitLoopAsync(AutoCommit auto)
    {
        var token = _lifetime.Token;
        while (true)
        {
            try
            {
                await _storeSignal.WaitAsync(auto.Interval ?? Timeout.InfiniteTimeSpan, token);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            if (await FlushCountedAsync() is { } error)
            {
                _logger.LogWarning(error, "The partition reader could not store a counted position.");
            }
        }
    }

    /// <summary>Stores the latest counted position of each partition ahead of its last store, one at a time.</summary>
    private async Task<Exception?> FlushCountedAsync()
    {
        List<Checkpoint> due;
        lock (_gate)
        {
            due =
            [
                .. _states.Values
                    .Where(state => state.Counted is { } counted && counted.Stamp?.Generation == state.Generation
                                                                && (state.Stored is null
                                                                    || state.Stored.Offset < counted.Offset))
                    .Select(state => state.Counted!)
            ];
        }

        Exception? failure = null;
        foreach (var checkpoint in due)
        {
            try
            {
                await _client.StoreConsumerPositionAsync(_settings.Consumer, _settings.StreamId, _settings.TopicId,
                    checkpoint);
                RecordStored(checkpoint);
            }
            catch (Exception error)
            {
                failure ??= error;
            }
        }

        return failure;
    }

    /// <summary>
    ///     With <see cref="CommitMode.OnRead" /> the server stored the offsets of batches that the application never
    ///     took, so each partition with one moves its offset back.
    /// </summary>
    private async Task<Exception?> RewindAsync()
    {
        List<(uint Partition, ulong? Offset, PartitionContext Context)> rewinds = [];
        lock (_gate)
        {
            var undelivered = new Dictionary<uint, PolledMessages>();
            foreach (var entry in _queue)
            {
                if (entry.Batch is { } batch && IsCurrent(entry))
                {
                    undelivered.TryAdd(batch.PartitionId, batch);
                }
            }

            foreach (var (partition, state) in _states)
            {
                var handedOver = state.Handed is { } handed
                                 && (state.Delivered is not { } last || last.Offset < handed.Offset);
                undelivered.TryGetValue(partition, out var queued);
                queued ??= state.CatchUpBatch;
                if (!handedOver && queued is null && state.PendingStart is null)
                {
                    continue;
                }

                if (state.Delivered is { } delivered)
                {
                    rewinds.Add((partition, delivered.Offset, delivered.Context));
                }
                else if (_settings.Start.Kind == MessagePolling.Next)
                {
                    // A lost read stored from its start, which lies before the batch that the next read held back.
                    var first = Math.Min(state.FirstHanded ?? queued?.Messages[0].Header.Offset ?? ulong.MaxValue,
                        state.PendingStart?.Value ?? ulong.MaxValue);
                    rewinds.Add((partition, first == 0 ? null : first - 1,
                        state.Handed?.Context ?? queued?.Context ?? state.PendingStart!.Value.Context!.Value));
                }
            }
        }

        Exception? failure = null;
        foreach (var (partition, offset, context) in rewinds)
        {
            try
            {
                if (offset is { } stored)
                {
                    await _client.StoreOffsetAsync(_settings.Consumer, _settings.StreamId, _settings.TopicId, stored,
                        partition, context);
                }
                else
                {
                    await DeleteOffsetAsync(partition, context, CancellationToken.None);
                }
            }
            catch (Exception error)
            {
                failure ??= error;
            }
        }

        return failure;
    }

    private async Task DeleteOffsetAsync(uint partition, PartitionContext? context, CancellationToken token)
    {
        try
        {
            if (context is { } known && _fetcher is not null)
            {
                await _client.DeleteOffsetAsync(_settings.Consumer, _settings.StreamId, _settings.TopicId, partition,
                    known, token);
            }
            else
            {
                await _client.DeleteOffsetAsync(_settings.Consumer, _settings.StreamId, _settings.TopicId, partition,
                    token);
            }
        }
        catch (IggyInvalidStatusCodeException error) when (error.StatusCode == ConsumerOffsetNotFound)
        {
        }
    }

    private async Task RunAsync()
    {
        var token = _lifetime.Token;
        while (!token.IsCancellationRequested)
        {
            TimeSpan wait;
            var failed = false;
            try
            {
                if (_assignment is not null)
                {
                    await RefreshAssignmentAsync(token);
                }

                wait = await StartReadsAsync(token);
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                return;
            }
            catch (Exception error)
            {
                if (IsTransient(error))
                {
                    _logger.LogWarning(error, "The partition reader retries a refused refresh.");
                }
                else
                {
                    Report(null, 0, error);
                }

                wait = _settings.RetryInterval;
                failed = true;
            }

            try
            {
                // A caller that asks again after a failure does not cut the retry interval short.
                if (failed)
                {
                    await Task.Delay(wait, token);
                }
                else
                {
                    await _wake.WaitAsync(wait, token);
                }
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    private async Task RefreshAssignmentAsync(CancellationToken token)
    {
        if (_client is ISessionGenerationProvider session && session.SessionGeneration != _joinedSession)
        {
            var generation = session.SessionGeneration;
            await _client.JoinConsumerGroupAsync(_settings.StreamId, _settings.TopicId, _settings.Consumer.ConsumerId,
                token);
            _joinedSession = generation;
            _resync = true;
        }

        bool resync;
        var now = Environment.TickCount64;
        lock (_gate)
        {
            // A joining member gets its partitions one at a time, so the reader asks the coordinator each interval.
            resync = _resync || _syncedAt is not { } synced
                             || now - synced >= (long)GroupRefreshInterval.TotalMilliseconds;
            _resync = false;
        }

        var assigned = await _assignment!.GetAssignedPartitionsAsync(_settings.StreamId, _settings.TopicId,
            _settings.Consumer.ConsumerId, resync, token);
        if (resync)
        {
            _syncedAt = now;
        }
        List<Checkpoint> leaving = [];
        lock (_gate)
        {
            var owned = assigned.ToHashSet();
            foreach (var partition in _states.Keys.Where(partition => !owned.Contains(partition)).ToList())
            {
                var state = _states[partition];
                if (state.Counted is { } counted && (state.Stored is null || state.Stored.Offset < counted.Offset))
                {
                    leaving.Add(counted);
                }

                GiveUpGap(partition, state);
                _states.Remove(partition);
                state.Idle.TrySetResult();
            }

            foreach (var partition in owned.Where(partition => !_states.ContainsKey(partition)))
            {
                _states[partition] = new PartitionState(_settings.Start, ++_generations);
            }
        }

        foreach (var checkpoint in leaving)
        {
            try
            {
                await _client.StoreConsumerPositionAsync(_settings.Consumer, _settings.StreamId, _settings.TopicId,
                    checkpoint, token);
            }
            catch (Exception error) when (error is not OperationCanceledException)
            {
                _logger.LogWarning(error, "Partition {Partition} left the reader before its counted position was stored.",
                    checkpoint.PartitionId);
            }
        }
    }

    /// <summary>
    ///     Starts a read for each partition that may read now, and returns how long the worker may sleep. Over TCP the
    ///     partitions of one primary share one request. A partition reads with its own poll for a start a request of
    ///     several partitions cannot take, while it reads again what a lost read stored, and while it reads a section
    ///     that did not decode one message at a time.
    /// </summary>
    private async Task<TimeSpan> StartReadsAsync(CancellationToken token)
    {
        var now = Environment.TickCount64;
        var nextRetry = long.MaxValue;
        List<uint> grouped = [];
        List<uint> alone = [];
        lock (_gate)
        {
            var reading = OnRead
                ? _demand && _queue.Count == 0
                : _queuedMessages < _settings.PrefetchMessages && _queuedBytes < _settings.PrefetchBytes;
            if (reading)
            {
                foreach (var (partition, state) in _states)
                {
                    if (state.InFlight || state.Stopped is not null)
                    {
                        continue;
                    }

                    if (state.RetryAt > now)
                    {
                        nextRetry = Math.Min(nextRetry, state.RetryAt);
                        continue;
                    }

                    if (_fetcher is not null && state.Position.Kind is MessagePolling.Offset or MessagePolling.Next
                                             && state.CatchUpUntil is null && state.AloneUntil is null
                                             && !NeedsStart(state.Position))
                    {
                        grouped.Add(partition);
                    }
                    else
                    {
                        alone.Add(partition);
                    }
                }
            }
        }

        foreach (var partition in alone)
        {
            StartPoll(partition);
        }

        if (grouped.Count > 0)
        {
            // A fetch reads its targets in order until it holds its count, so the partitions served last go last.
            lock (_gate)
            {
                grouped = [.. grouped.OrderBy(partition => _states.TryGetValue(partition, out var state) ? state.ServedAt : 0)];
            }

            var groups = await _fetcher!.ReadinessGroupsAsync(_settings.StreamId, _settings.TopicId, grouped, token);
            foreach (var fetch in Spread(groups, alone.Count))
            {
                StartFetch(fetch);
            }
        }

        var wait = nextRetry == long.MaxValue
            ? Timeout.InfiniteTimeSpan
            : TimeSpan.FromMilliseconds(Math.Max(1, nextRetry - Environment.TickCount64));
        if (_assignment is not null && (wait == Timeout.InfiniteTimeSpan || wait > GroupRefreshInterval))
        {
            wait = GroupRefreshInterval;
        }

        return wait;
    }

    /// <summary>
    ///     Deals the backlogged partitions of each group over the fetch slots that are free, one fetch with the full
    ///     limits each, so a backlog keeps as many reads in flight as the prefetch holds. Every group gets one fetch
    ///     first, and its quiet partitions share one fetch, which may wait on the server. With
    ///     <see cref="CommitMode.OnRead" /> a turn starts only while <see cref="NextAsync" /> waits, and a partition
    ///     is in one committing read at a time, so the fetches of a turn commit disjoint partitions.
    /// </summary>
    private List<IReadOnlyList<uint>> Spread(IReadOnlyList<IReadOnlyList<uint>> groups, int alone)
    {
        List<(List<uint> Backlogged, List<uint> Quiet)> split;
        int free;
        lock (_gate)
        {
            split =
            [
                .. groups.Select(group => (
                    group.Where(partition => _states.TryGetValue(partition, out var state) && state.Backlogged).ToList(),
                    group.Where(partition => !_states.TryGetValue(partition, out var state) || !state.Backlogged).ToList()))
            ];
            var slots = Math.Min(_settings.PrefetchMessages / Math.Max(_settings.Options.Count, 1),
                _settings.PrefetchBytes / Math.Max(_settings.Options.MaxBytes, 1));
            free = (int)Math.Clamp(slots, 1, MaxFetchSlots) - _fetching - groups.Count - alone;
        }

        // Every fetch holds at least one partition, so a group without quiet partitions keeps one backlogged.
        var extras = new int[split.Count];
        for (var dealt = true; dealt && free > 0;)
        {
            dealt = false;
            for (var index = 0; index < split.Count && free > 0; index++)
            {
                var (backlogged, quiet) = split[index];
                if (extras[index] < backlogged.Count - (quiet.Count > 0 ? 0 : 1))
                {
                    extras[index]++;
                    free--;
                    dealt = true;
                }
            }
        }

        List<IReadOnlyList<uint>> fetches = [];
        for (var index = 0; index < split.Count; index++)
        {
            var (backlogged, quiet) = split[index];
            if (extras[index] == 0)
            {
                fetches.Add(groups[index]);
                continue;
            }

            if (quiet.Count > 0)
            {
                fetches.Add(quiet);
            }

            var shares = quiet.Count > 0 ? extras[index] : extras[index] + 1;
            fetches.AddRange(backlogged.Select((partition, position) => (partition, position))
                .GroupBy(entry => entry.position % shares, entry => entry.partition)
                .Select(share => (IReadOnlyList<uint>)[.. share]));
        }

        return fetches;
    }

    private void StartPoll(uint partition)
    {
        Target target;
        bool commits;
        bool single;
        bool check;
        ulong? catchUpUntil;
        lock (_gate)
        {
            if (Claim(partition) is not { } claimed)
            {
                return;
            }

            target = claimed;
            var state = _states[partition];
            single = state.AloneUntil is not null;
            check = NeedsStart(target.Sent);
            catchUpUntil = target.Sent.Kind == MessagePolling.Offset && state.CatchUpUntil is { } until
                                                                     && target.Sent.Value <= until
                ? until
                : null;
            commits = OnRead && state.CatchUpUntil is null && !single && !check;
            if (commits && target.Sent.Context is not null)
            {
                state.PendingStart ??= target.Sent;
            }
        }

        Track(PollAsync(target, commits, single, check, catchUpUntil));
    }

    private void StartFetch(IReadOnlyList<uint> group)
    {
        List<Target> targets = [];
        lock (_gate)
        {
            foreach (var partition in group)
            {
                if (Claim(partition) is { } target)
                {
                    targets.Add(target);
                    if (OnRead && target.Sent.Context is not null)
                    {
                        _states[partition].PendingStart ??= target.Sent;
                    }
                }
            }
        }

        if (targets.Count > 0)
        {
            lock (_gate)
            {
                _fetching++;
            }

            Track(FetchAsync(targets));
        }
    }

    /// <summary>Marks an idle partition as reading, under the lock.</summary>
    private Target? Claim(uint partition)
    {
        if (!_states.TryGetValue(partition, out var state) || state.InFlight || state.Stopped is not null)
        {
            return null;
        }

        state.InFlight = true;
        state.Claim = ++_claims;
        state.Idle = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        return new Target(partition, state.Position, state.Generation, state.Claim);
    }

    private void Track(Task read)
    {
        lock (_gate)
        {
            _reads.Add(read);
        }

        read.ContinueWith(completed =>
        {
            lock (_gate)
            {
                _reads.Remove(completed);
            }

            Wake();
        }, TaskScheduler.Default);
    }

    private async Task FetchAsync(List<Target> targets)
    {
        try
        {
            await FetchOnceAsync(targets);
        }
        finally
        {
            lock (_gate)
            {
                _fetching--;
            }
        }
    }

    private async Task FetchOnceAsync(List<Target> targets)
    {
        FetchedMessages reply;
        try
        {
            reply = await _fetcher!.FetchMessagesAsync(_settings.StreamId, _settings.TopicId,
                targets.ConvertAll(target => (target.Partition, target.Sent)), _settings.Consumer,
                _settings.Options.Count, OnRead, _settings.Options.Wire, _lifetime.Token);
        }
        catch (Exception error)
        {
            await AbandonLostAsync(error);
            foreach (var target in targets)
            {
                Refuse(target, error);
            }

            return;
        }

        using (reply)
        {
            List<(Target Target, FetchedSection? Section, PolledMessages? Polled, Exception? Error)> outcomes =
                new(targets.Count);
            foreach (var target in targets)
            {
                var section = reply.Sections.FirstOrDefault(section => section.PartitionId == target.Partition);
                if (section is null || section.Status != 0)
                {
                    outcomes.Add((target, section, null, section?.Refusal()));
                    continue;
                }

                try
                {
                    using var rental = section.DecodeRented();
                    outcomes.Add((target, section, BinaryMapper.MaterializeMessages(rental), null));
                }
                catch (Exception error) when (error is MessageDecryptionException or MalformedResponseException)
                {
                    outcomes.Add((target, section, null, error));
                }
            }

            foreach (var outcome in outcomes.Where(outcome => outcome.Section is { Status: not 0 }))
            {
                await AbandonLostAsync(outcome.Error!);
            }

            // Every target of the reply settles before a new read starts, so the next fetch keeps the turn order.
            lock (_gate)
            {
                foreach (var (target, section, polled, error) in outcomes)
                {
                    if (section is null)
                    {
                        // A read that does not wait would ask the server again at once.
                        Retry(target,
                            _settings.Options.MaxWait == TimeSpan.Zero ? (long)_settings.RetryInterval.TotalMilliseconds : 0);
                    }
                    else if (section.Status != 0)
                    {
                        Refuse(target, error!);
                    }
                    else if (polled is null)
                    {
                        ReadAlone(target, section, error!);
                    }
                    else
                    {
                        Complete(target, polled, section.NextOffset);
                    }
                }
            }
        }
    }

    private bool NeedsStart(PollingStrategy position)
    {
        return OnRead && position.Kind == MessagePolling.Next && position.Context is null;
    }

    private async Task PollAsync(Target target, bool commits, bool single, bool check, ulong? catchUpUntil)
    {
        var options = _settings.Options with
        {
            Commit = commits ? CommitPolicy.OnRead : CommitPolicy.Manual,
            Count = single || check ? 1 : _settings.Options.Count,
            MinCount = single || check ? 1 : _settings.Options.MinCount
        };
        if (!single && !check && catchUpUntil is { } until)
        {
            // The server stored these messages already, so the read takes them and no more.
            options = options with
            {
                Count = (uint)Math.Min(until - target.Sent.Value + 1, options.Count),
                MinCount = 1,
                MaxWait = TimeSpan.Zero
            };
        }
        PolledMessages polled;
        try
        {
            polled = await _client.PollMessagesAsync(_settings.StreamId, _settings.TopicId, target.Partition,
                _settings.Consumer, target.Sent, options, _lifetime.Token);
        }
        catch (Exception error) when (target.Sent.Kind == MessagePolling.Offset
                                      && error is MessageDecryptionException or MalformedResponseException)
        {
            if (single)
            {
                Undecodable(target, error);
            }
            else
            {
                ReadAloneFrom(target, error);
            }

            return;
        }
        catch (Exception error) when (commits && error is MalformedResponseException)
        {
            // The server stored this read, so the next read finds the range it skipped and reads it again by offset.
            lock (_gate)
            {
                if (Settle(target) is { } state)
                {
                    state.Lost = true;
                    state.RetryAt = Environment.TickCount64 + (long)_settings.RetryInterval.TotalMilliseconds;
                }
            }

            _logger.LogWarning(error, "A committing read of partition {Partition} from offset {Offset} does not decode.",
                target.Partition, target.Sent.Value);
            return;
        }
        catch (Exception error)
        {
            await AbandonLostAsync(error);
            Refuse(target, error);
            return;
        }

        if (check)
        {
            lock (_gate)
            {
                if (Settle(target) is { } state)
                {
                    if (polled.Messages.Count > 0)
                    {
                        state.Position = PollingStrategy.Continuation(target.Partition, MessagePolling.Next,
                            polled.Messages[0].Header.Offset, polled.Context);
                    }
                    else if (_settings.Options.MaxWait == TimeSpan.Zero)
                    {
                        state.RetryAt = Environment.TickCount64 + (long)_settings.RetryInterval.TotalMilliseconds;
                    }
                }
            }

            return;
        }

        Complete(target, polled);
    }

    /// <summary>
    ///     Abandons the operation of a lost committing read of this reader, so its partitions read again. The next
    ///     read finds what it stored.
    /// </summary>
    private async Task AbandonLostAsync(Exception error)
    {
        if (error is not VsrFetchProgressUncertainException { OperationId: { } operation })
        {
            return;
        }

        try
        {
            await _client.AbandonOperationAsync(operation);
        }
        catch (Exception abandon)
        {
            _logger.LogWarning(abandon, "The partition reader could not abandon its lost read {Operation}.", operation);
        }
    }

    /// <summary>Hands a reply of a read to the application and moves the position of its partition after it.</summary>
    private void Complete(Target target, PolledMessages polled, ulong? nextOffset = null)
    {
        lock (_gate)
        {
            if (Settle(target) is not { } state)
            {
                // The server stored this read, so a partition that left the assignment still hands it over.
                if (OnRead && polled.Messages.Count > 0 && Keeps(target.Partition, target.Generation))
                {
                    EnqueueBatch(target, polled);
                }

                return;
            }

            state.Lasting = 0;
            // A fetch section that ended before the head of its partition reads again at once.
            state.Backlogged = nextOffset is { } sectionEnd && sectionEnd <= polled.CurrentOffset
                                                            && (polled.Messages.Count > 0 || polled.CurrentOffset > 0);
            if (polled.Messages.Count == 0)
            {
                var emptyNext = nextOffset ?? polled.CurrentOffset + 1;
                if (OnRead && state.Lost && target.Sent is { Kind: MessagePolling.Next, Context: { } context }
                           && SameHistory(context, polled.Context) && emptyNext > target.Sent.Value)
                {
                    state.Position = PollingStrategy.Continuation(target.Partition, MessagePolling.Offset,
                        target.Sent.Value, polled.Context);
                    state.CatchUpUntil = emptyNext - 1;
                    return;
                }

                if (target.Sent.Kind != MessagePolling.Next || !OnRead)
                {
                    polled.RequestStart = target.Sent;
                    state.Position = polled.Context == default ? target.Sent : polled.Continuation;
                }

                // A read again of a lost range does not wait either.
                if (_settings.Options.MaxWait == TimeSpan.Zero || state.CatchUpUntil is not null)
                {
                    state.RetryAt = Environment.TickCount64 + (long)_settings.RetryInterval.TotalMilliseconds;
                }

                return;
            }

            var first = polled.Messages[0].Header.Offset;
            var last = polled.Messages[^1].Header.Offset;
            if (OnRead && state.Lost && target.Sent is { Kind: MessagePolling.Next, Context: { } sentContext }
                       && first > target.Sent.Value && SameHistory(sentContext, polled.Context))
            {
                _logger.LogWarning(
                    "Partition {Partition} skipped offsets from {From} to {Until}, which a lost read stored. They are read again without a commit.",
                    target.Partition, target.Sent.Value, last);
                state.Position = PollingStrategy.Continuation(target.Partition, MessagePolling.Offset,
                    target.Sent.Value, polled.Context);
                state.CatchUpUntil = last;
                state.CatchUpBatch = polled;
                return;
            }

            state.PendingStart = null;
            // This read stored its offset, which fences out the lost read.
            state.Lost &= target.Sent.Kind != MessagePolling.Next;
            var next = last + 1;
            var byOffset = state.AloneUntil is { } aloneUntil && next < aloneUntil
                           || state.CatchUpUntil is { } catchUpUntil && next <= catchUpUntil;
            if (state.AloneUntil is { } until && next >= until)
            {
                state.AloneUntil = null;
            }

            if (state.CatchUpUntil is { } end && next > end)
            {
                state.CatchUpUntil = null;
                state.CatchUpBatch = null;
            }

            state.Position = PollingStrategy.Continuation(target.Partition,
                OnRead && !byOffset ? MessagePolling.Next : MessagePolling.Offset, next, polled.Context);
            state.ServedAt = ++_serves;
            EnqueueBatch(target, polled);
        }
    }

    private void EnqueueBatch(Target target, PolledMessages polled)
    {
        polled.RequestStart = target.Sent;
        polled.Stamp = new ReaderStamp(_id, target.Generation);
        Enqueue(new Entry(target.Partition, target.Generation, polled, null,
            polled.Messages.Count, polled.Messages.Sum(message => (long)message.Payload.Length)));
    }

    /// <summary>A section stored nothing that can be read: its partition reads it again one message at a time.</summary>
    private void ReadAlone(Target target, FetchedSection section, Exception error)
    {
        lock (_gate)
        {
            if (Settle(target) is not { } state)
            {
                return;
            }

            var start = section.NextOffset - Math.Min(section.NextOffset, section.MessagesCount);
            _logger.LogWarning(error,
                "A batch of partition {Partition} from offset {Offset} does not decode. It is read one message at a time.",
                target.Partition, start);
            state.Position = PollingStrategy.Continuation(target.Partition, MessagePolling.Offset, start,
                section.Context);
            state.AloneUntil = section.NextOffset;
        }
    }

    /// <summary>A poll from an offset did not decode, so its partition reads that range one message at a time.</summary>
    private void ReadAloneFrom(Target target, Exception error)
    {
        lock (_gate)
        {
            if (Settle(target) is not { } state)
            {
                return;
            }

            _logger.LogWarning(error,
                "A batch of partition {Partition} from offset {Offset} does not decode. It is read one message at a time.",
                target.Partition, target.Sent.Value);
            state.AloneUntil = target.Sent.Value + _settings.Options.Count;
        }
    }

    /// <summary>The message at the sent offset does not decode. The partition reads it again until a seek past it.</summary>
    private void Undecodable(Target target, Exception error)
    {
        PollingStrategy skip;
        lock (_gate)
        {
            if (Settle(target) is not { } state)
            {
                return;
            }

            state.AloneUntil ??= target.Sent.Value + 1;
            state.RetryAt = Environment.TickCount64 + (long)_settings.RetryInterval.TotalMilliseconds;
            skip = PollingStrategy.Continuation(target.Partition, MessagePolling.Offset, target.Sent.Value + 1,
                target.Sent.Context ?? default);
        }

        Report(target.Partition, target.Generation, error, skip);
    }

    /// <summary>
    ///     A read that did not return its partition. A refusal before the read retries, and only a lasting or a
    ///     history refusal stops the partition until a seek.
    /// </summary>
    private void Refuse(Target target, Exception error)
    {
        ReadException? report = null;
        lock (_gate)
        {
            if (Settle(target) is not { } state || _lifetime.IsCancellationRequested)
            {
                return;
            }

            var now = Environment.TickCount64;
            var interval = (long)_settings.RetryInterval.TotalMilliseconds;
            switch (error)
            {
                case VsrRequestOutcomeUnknownException:
                    // A lost reply. With an on read policy the next read finds what it stored.
                    state.Lost |= OnRead && error is VsrFetchProgressUncertainException;
                    state.RetryAt = now + interval;
                    return;
                case IggyInvalidStatusCodeException { StatusCode: VsrError.NOT_PRIMARY }:
                    return;
                case IggyInvalidStatusCodeException { StatusCode: VsrError.CONSUMER_GROUP_PARTITION_NOT_OWNED }:
                    GiveUpGap(target.Partition, state);
                    // A rejoin gives the partition a new owner generation, which the old position still names.
                    state.Position = _settings.Start;
                    state.PendingStart = null;
                    state.Lost = false;
                    _resync = true;
                    state.Lasting++;
                    if (state.Lasting >= MaxLastingRefusals)
                    {
                        report = new ReadException(target.Partition, error);
                        state.Stopped = report;
                        break;
                    }

                    state.RetryAt = now + interval * (1L << (state.Lasting - 1));
                    return;
                case IggyInvalidStatusCodeException { StatusCode: VsrError.HISTORY_UNAVAILABLE }
                    when target.Sent.Context is null:
                    _logger.LogWarning(
                        "Partition {Partition} was refused with a history that the reader never read. It reads again.",
                        target.Partition);
                    state.RetryAt = now + interval;
                    return;
                case IggyInvalidStatusCodeException { StatusCode: VsrError.HISTORY_UNAVAILABLE or VsrError.INVALID_SIZE_BYTES }:
                    report = new ReadException(target.Partition, error);
                    state.Stopped = report;
                    break;
                case IggyInvalidStatusCodeException status when IsLasting(status.StatusCode):
                    state.Lasting++;
                    if (state.Lasting >= MaxLastingRefusals)
                    {
                        report = new ReadException(target.Partition, error);
                        state.Stopped = report;
                        break;
                    }

                    _logger.LogWarning(error, "Partition {Partition} was refused, it reads again later.",
                        target.Partition);
                    state.RetryAt = now + interval * (1L << (state.Lasting - 1));
                    return;
                default:
                    if (IsTransient(error))
                    {
                        state.RetryAt = now + interval;
                        return;
                    }

                    report = IsReaderWide(error) ? null : new ReadException(target.Partition, error);
                    state.RetryAt = now + interval;
                    break;
            }
        }

        if (report is not null)
        {
            Enqueue(target.Partition, target.Generation, report);
        }
        else
        {
            Report(null, 0, error);
        }
    }

    private void GiveUpGap(uint partition, PartitionState state)
    {
        if (state.CatchUpUntil is not { } until)
        {
            return;
        }

        var held = state.CatchUpBatch;
        var target = new Target(partition, state.Position, state.Generation);
        var first = held?.Messages[0].Header.Offset ?? until + 1;
        if (state.Position.Value < first)
        {
            Enqueue(partition, state.Generation, new ReadException(partition,
                VsrError.Exception(VsrError.FETCH_PROGRESS_UNCERTAIN,
                    $"A lost read stored offsets {state.Position.Value} to {first - 1} of partition " +
                    $"{partition}, and the partition moved before they were read again.")));
        }

        var next = held is null ? until + 1 : held.Messages[^1].Header.Offset + 1;
        if (held is not null)
        {
            held.Messages = held.Messages.Where(message => message.Header.Offset >= state.Position.Value).ToArray();
        }

        state.CatchUpUntil = null;
        state.CatchUpBatch = null;
        state.Position = PollingStrategy.Continuation(partition, MessagePolling.Next, next,
            held?.Context ?? state.Position.Context!.Value);
        if (held is { Messages.Count: > 0 })
        {
            EnqueueBatch(target, held);
        }
    }

    private void Retry(Target target, long delay)
    {
        lock (_gate)
        {
            if (Settle(target) is { } state)
            {
                state.RetryAt = delay == 0 ? 0 : Environment.TickCount64 + delay;
            }
        }
    }

    /// <summary>
    ///     Ends the read of a target, under the lock. Null when the generation of its partition moved on. The state of
    ///     a partition assigned again has a read of its own, which the old read leaves in flight.
    /// </summary>
    private PartitionState? Settle(Target target)
    {
        if (!_states.TryGetValue(target.Partition, out var state) || state.Claim != target.Claim)
        {
            return null;
        }

        state.InFlight = false;
        state.Backlogged = false;
        state.Idle.TrySetResult();
        return state.Generation == target.Generation ? state : null;
    }

    private void Report(uint? partition, ulong generation, Exception error, PollingStrategy? skip = null)
    {
        lock (_gate)
        {
            // One error of the whole reader at a time, so a lasting outage does not fill the queue.
            if (partition is null && _queue.Any(entry => entry.Error is { PartitionId: null }))
            {
                return;
            }
        }

        Enqueue(partition, generation, new ReadException(partition, error, skip));
    }

    private void Enqueue(uint? partition, ulong generation, ReadException error)
    {
        lock (_gate)
        {
            Enqueue(new Entry(partition, generation, null, error, 0, 0));
        }
    }

    private void Enqueue(Entry entry)
    {
        _queue.AddLast(entry);
        _queuedMessages += entry.Messages;
        _queuedBytes += entry.Bytes;
        _arrival.TrySetResult();
        _arrival = NewArrival();
    }

    private void Release(Entry entry)
    {
        _queuedMessages -= entry.Messages;
        _queuedBytes -= entry.Bytes;
    }

    private bool IsCurrent(Entry entry)
    {
        return entry.PartitionId is not { } partition || Keeps(partition, entry.Generation);
    }

    /// <summary>
    ///     Whether a read of a partition under a generation still counts. A seek or a new assignment of the partition
    ///     drops it. A partition that left the assignment keeps the reads that the server stored under
    ///     <see cref="CommitMode.OnRead" />.
    /// </summary>
    private bool Keeps(uint partition, ulong generation)
    {
        return (_states.TryGetValue(partition, out var state) && state.Generation == generation) ||
               (OnRead && (!_sought.TryGetValue(partition, out var sought) || generation >= sought));
    }

    private PartitionState State(uint partition)
    {
        return _states.TryGetValue(partition, out var state)
            ? state
            : throw VsrError.Exception(VsrError.INVALID_COMMAND, $"The reader does not read partition {partition}.");
    }

    private void Wake()
    {
        Signal(_wake);
    }

    private static void Signal(SemaphoreSlim signal)
    {
        try
        {
            if (signal.CurrentCount == 0)
            {
                signal.Release();
            }
        }
        catch (SemaphoreFullException)
        {
        }
    }

    private static bool SameHistory(PartitionContext sent, PartitionContext served)
    {
        return sent.Incarnation == served.Incarnation && sent.OwnerGeneration == served.OwnerGeneration;
    }

    private static bool IsTransient(Exception error)
    {
        return error is IggyInvalidStatusCodeException
        {
            StatusCode: VsrError.TRANSIENT_NOT_ACCEPTED or VsrError.TRANSIENT_NOT_COMMITTED or VsrError.NOT_PRIMARY
            or VsrError.DEFERRED_POLL_CAPACITY_EXCEEDED or VsrError.LIFECYCLE_BUSY
        } or VsrRequestOutcomeUnknownException;
    }

    /// <summary>A refusal that a retry soon after does not change.</summary>
    private static bool IsLasting(int status)
    {
        return status is VsrError.UNAUTHORIZED or VsrError.FEATURE_UNAVAILABLE or VsrError.INVALID_COMMAND
            or VsrError.PARTITION_NOT_FOUND or VsrError.STREAM_ID_NOT_FOUND or VsrError.STREAM_NAME_NOT_FOUND
            or VsrError.TOPIC_ID_NOT_FOUND or VsrError.TOPIC_NAME_NOT_FOUND;
    }

    /// <summary>An error of the connection or the session, which no partition causes.</summary>
    private static bool IsReaderWide(Exception error)
    {
        return error is NotConnectedException or ObjectDisposedException or IOException
            or VsrSessionEvictedException
            || error is IggyInvalidStatusCodeException
            {
                StatusCode: VsrError.UNAUTHENTICATED or VsrError.STALE_CLIENT
            };
    }

    /// <summary>A read of one partition. Its claim tells it from a later read of the partition, and zero reads nothing.</summary>
    private readonly record struct Target(uint Partition, PollingStrategy Sent, ulong Generation, ulong Claim = 0);

    /// <summary>A batch or an error, with the generation of its partition when the reader read it.</summary>
    private sealed record Entry(
        uint? PartitionId,
        ulong Generation,
        PolledMessages? Batch,
        ReadException? Error,
        long Messages,
        long Bytes);

    private sealed class PartitionState(PollingStrategy start, ulong generation)
    {
        internal ulong? AloneUntil;

        /// <summary>The last fetch section of the position ended before the head of the partition.</summary>
        internal bool Backlogged;
        internal ulong? CatchUpUntil;
        internal PolledMessages? CatchUpBatch;
        internal ulong Claim;
        internal PollingStrategy? PendingStart;
        internal Checkpoint? Counted;
        internal uint CountedSinceStore;
        internal (ulong Offset, PartitionContext Context)? Delivered;
        internal ulong? FirstHanded;
        internal ulong Generation = generation;
        internal (ulong Offset, PartitionContext Context)? Handed;
        internal TaskCompletionSource Idle = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal bool InFlight;
        internal int Lasting;

        /// <summary>With an on read policy: a committing read of the partition lost its reply and may have stored.</summary>
        internal bool Lost;
        internal PollingStrategy Position = start;
        internal long RetryAt;

        /// <summary>When a read last served messages of the partition, so served partitions go last in a fetch.</summary>
        internal ulong ServedAt;

        internal ReadException? Stopped;
        internal Checkpoint? Stored;
    }
}
