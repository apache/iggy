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
///             <see cref="CloseAsync(CancellationToken)" />. It also joins again when the group no longer holds its
///             member, for example after the group was deleted and created again, and reports no error for that. A
///             join that the server refuses is an error of the reader.
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
///             while the application waits, and <see cref="CloseAsync(CancellationToken)" /> moves the offset back
///             to the last delivered message. With <see cref="CommitMode.Auto" />, the reader counts each batch as its
///             <see cref="CommitAfter" /> says, or each <see cref="Mark" />, and stores the latest counted position at
///             the interval or the event of the policy and in <see cref="CloseAsync(CancellationToken)" />.
///         </item>
///         <item>
///             The reader stays usable after a <see cref="ReadException" /> and retries by itself. Status 87
///             (HistoryUnavailable) stops its partition until a <see cref="SeekAsync" /> when the position came from an
///             earlier read of a history that is gone. Status 96 (TieredReadFailed) stops its partition at once,
///             because no retry changes it. A refusal that a quick retry does not change, such as status 41
///             (Unauthorized), 3007 (PartitionNotFound) or 5009 (ConsumerGroupPartitionNotOwned), is retried after a
///             wait that doubles each time, and after six in a row it stops its partition until a
///             <see cref="SeekAsync" /> with one error. Status 5009 never stops it: each sixth in a row reports one
///             error, and the partition reads on. A new session starts the count and the wait again, and so does a
///             group that no longer holds the member of the reader. Status 5009
///             alone is not a leave: the partition keeps its position and the state that the close needs, the reader
///             asks for the assignment again, and the partition sends owner generation 0 until its next reply. Only an
///             assignment without the partition drops that state. An offset write refused with status 5009 reads its
///             partition once without a commit, and when that read is served in the incarnation of the write, the
///             write goes once more under the owner of the reply. After a read whose outcome is unknown the partition
///             waits the retry interval. With
///             <see cref="CommitMode.OnRead" />, a read whose reply was lost is repaired: the next read finds the
///             messages that the lost read stored and reads them again without a commit before it goes on. A read
///             that starts past its position while no read was lost delivers from there.
///         </item>
///         <item>One caller at a time calls <see cref="NextAsync" />. The other members are safe to call concurrently.</item>
///     </list>
/// </remarks>
public sealed class PartitionReader : IAsyncDisposable
{
    private const int MaxLastingRefusals = 6;

    /// <summary>The most doublings of the retry interval between lookups of the groups, which never stop.</summary>
    private const int MaxRegroupBackoffSteps = 5;

    private const int MinPrefetchBatches = 32;
    private const int MaxRequestsInFlight = 32;
    private const int ConsumerOffsetNotFound = 3021;
    internal static readonly TimeSpan GroupRefreshInterval = TimeSpan.FromSeconds(1);

    /// <summary>
    ///     How long after its start a close reads a partition for its owner again, so a close whose caller never
    ///     cancels, such as <see cref="DisposeAsync" />, still ends while those reads stay refused.
    /// </summary>
    internal static readonly TimeSpan CloseOwnerReadBudget = TimeSpan.FromSeconds(10);

    /// <summary>
    ///     The pause before the next sync after a join, or after a sync that changed the partitions of the member. A
    ///     join moves ownership only after the partition logs persist the new owner.
    /// </summary>
    private static readonly TimeSpan AssignmentSettlePause = TimeSpan.FromMilliseconds(10);

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
    private readonly long _prefetchBytes;
    private readonly long _prefetchMessages;
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
    private int _groupCursor;

    /// <summary>
    ///     The groups of the last lookup, without the partitions that left or whose route changed since. A lookup runs
    ///     apart from the reads, so a slow or failed one holds no read and keeps these groups.
    /// </summary>
    private IReadOnlyList<IReadOnlyList<uint>> _groups = [];

    private readonly HashSet<uint> _grouped = [];
    private Task _regrouping = Task.CompletedTask;

    /// <summary>The assignment or a route changed since the last lookup of the groups started.</summary>
    private bool _regroupDue = true;

    /// <summary>When the next lookup of the groups may start after a failed one.</summary>
    private long _regroupAt;

    private int _regroupRefusals;

    /// <summary>The last lookup of the groups failed, so the partitions in no group read with their own polls.</summary>
    private bool _regroupFailed;

    /// <summary>The start lookups in flight, under the lock.</summary>
    private int _lookups;

    private Task _lookingUp = Task.CompletedTask;

    private uint? _pollCursor;

    /// <summary>
    ///     The seeks in progress, under the lock, in the order they started. No read of their partitions starts, and a
    ///     seek runs after the earlier seeks of its partition.
    /// </summary>
    private readonly List<(uint Partition, Task Done)> _seeks = [];

    /// <summary>
    ///     The committing reads in flight of each partition, under the lock, of every generation and also from before
    ///     the partition left the assignment. A read counts until its task ends, so until the client released its
    ///     request and the reader abandoned a lost one.
    /// </summary>
    private readonly Dictionary<uint, CommittingReads> _committing = [];

    private ulong _claims;
    private ulong _generations;
    private ulong _serves;
    /// <summary>The session of the last join, under the lock: a refresh writes it and the worker reads it.</summary>
    private ulong _joinedSession;

    /// <summary>
    ///     A sync found no member of the client in the group, so the next refresh joins again. Under the lock, as
    ///     <see cref="_joinedSession" />.
    /// </summary>
    private bool _memberLost;

    /// <summary>The session whose refusals the partitions count, which only the worker reads and writes.</summary>
    private ulong _refusalSession;

    private long _queuedBytes;
    private long _queuedMessages;

    /// <summary>
    ///     The refresh of the assignment in flight. It runs apart from the reads, as in the Rust reader, so a slow or
    ///     failed one holds no read of the assignment that the last refresh confirmed.
    /// </summary>
    private Task _refreshing = Task.CompletedTask;

    /// <summary>When the next refresh of the assignment may start after a failed one, under the lock.</summary>
    private long _refreshAt;

    private bool _resync;

    /// <summary>
    ///     When the last sync or change of the assignment came, under the lock, or null when none came since the last
    ///     join.
    /// </summary>
    private long? _syncedAt;

    /// <summary>The pause after <see cref="_syncedAt" /> before the next sync of the assignment.</summary>
    private TimeSpan _syncPause = AssignmentSettlePause;

    private (Checkpoint Checkpoint, uint Messages)? _uncounted;

    internal PartitionReader(IIggyClient client, IConsumerGroupAssignmentProvider? assignment,
        ReaderSettings settings, ILogger logger)
    {
        _client = client;
        _assignment = assignment;
        _fetcher = client as IPartitionFetchClient;
        _settings = settings with { Start = WithoutZeroContext(settings.Start) };
        _prefetchBytes = Math.Max(settings.PrefetchBytes, (long)settings.Options.MaxBytes * MinPrefetchBatches);
        _prefetchMessages = Math.Max(settings.PrefetchMessages, (long)settings.Options.Count * MinPrefetchBatches);
        _logger = logger;
        _joinedSession = Session;
        _refusalSession = _joinedSession;
        foreach (var partition in settings.Partitions)
        {
            _states[partition] = NewState();
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

    /// <summary>
    ///     With <see cref="CommitMode.OnRead" /> a read moves on from the stored offset, so a start other than Next
    ///     applies only to a partition without a stored offset, which a lookup finds first.
    /// </summary>
    private bool LooksUpStart => OnRead && _settings.Start.Kind != MessagePolling.Next;

    /// <summary>The requests in flight, under the lock: the tracked reads and the start lookups.</summary>
    private int Requests => _reads.Count + _lookups;

    /// <summary>The polls in flight, under the lock: the requests that are not fetches.</summary>
    private int Polling => Requests - _fetching;

    private AutoCommit? AutoSettings => _settings.Options.Commit.Settings;

    /// <summary>The session generation of the client, which moves on with each sign-in and each new session.</summary>
    private ulong Session => (_client as ISessionGenerationProvider)?.SessionGeneration ?? 0;

    /// <summary>The latest metadata op of the client, which a write with owner generation 0 routes by.</summary>
    private ulong Watermark => _assignment?.MetadataWatermark ?? 0;

    /// <summary>Closes the reader as <see cref="CloseAsync()" /> does, and logs the error of the close instead of throwing it.</summary>
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
    ///     partition, or at or below the last commit of its generation, stores nothing. A store that the server
    ///     refuses as not owned reads its partition for the installed owner and goes once more under that owner. That
    ///     read goes again after each transient refusal until <paramref name="token" /> is canceled or
    ///     <see cref="CloseAsync(CancellationToken)" /> starts. Then the commit fails with the refusal, and its store
    ///     does not go again.
    /// </summary>
    /// <exception cref="IggyInvalidStatusCodeException">
    ///     Status 3 with <see cref="CommitMode.OnRead" />, and for a checkpoint that this reader did not deliver. The
    ///     errors of the offset store, such as status 5009 (ConsumerGroupPartitionNotOwned).
    /// </exception>
    /// <exception cref="VsrRequestOutcomeUnknownException">
    ///     The store has an unknown outcome. The reader releases it in the client, so a retry of the commit can store.
    /// </exception>
    public async Task CommitAsync(Checkpoint checkpoint, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(checkpoint);
        if (OnRead)
        {
            throw VsrError.Exception(VsrError.INVALID_COMMAND, "The server stores the offsets of an on read policy.");
        }

        ThrowIfForeign(checkpoint);
        Checkpoint owned;
        lock (_gate)
        {
            if (!_states.TryGetValue(checkpoint.PartitionId, out var state)
                || state.Generation != checkpoint.Stamp!.Value.Generation
                || state.Stored is { } stored && stored.Offset >= checkpoint.Offset)
            {
                return;
            }

            owned = Owned(state, checkpoint);
        }

        using var retry = LinkToCloseStart(token);
        try
        {
            await StoreAsync(owned, token, retry.Token);
        }
        catch (VsrRequestOutcomeUnknownException error)
        {
            // The client holds the store until an abandon, which would refuse a retry of the commit.
            await AbandonLostAsync(error);
            throw;
        }

        RecordStored(checkpoint);
    }

    /// <summary>
    ///     Sets the next read position of <paramref name="partitionId" /> and starts a new generation of it. It drops
    ///     the queued batches of the partition, ignores its late replies, and resumes a stopped partition. With
    ///     <see cref="CommitMode.OnRead" />, a seek to an offset above 0 stores the offset before it on the server, so
    ///     a restart resumes at the sought offset. If the partition has a stored offset, a seek to offset 0 or First
    ///     deletes it, because a read never moves the offset back. A seek to a timestamp first reads one message from
    ///     the timestamp without a commit and then seeks to the offset of that message. Without a message it stores
    ///     nothing, and neither does any other seek. No read of the partition starts while the seek runs. The seek
    ///     waits for every committing read of the partition in flight before it writes, also for a read from before the
    ///     partition left the assignment and came back. It moves only after its writes succeed. A failed read, store,
    ///     lookup or delete fails the seek, and the next read goes on from the old position. A store or delete whose
    ///     outcome stays unknown can still move the stored offset, so the reader then treats it as a lost read: a next
    ///     read that jumps past the old position reads the skipped messages again. The seek looks up the stored offset
    ///     first. After an unknown outcome, the close restores the offset from before the write. That is the last
    ///     delivered message, or the offset before an undelivered batch or a lost read. Without these, it is the stored
    ///     offset that the seek found. Without a stored offset, the close deletes the offset. Before the first read of
    ///     the partition, the reader also reads on after that stored offset. A cancel before the first write stops the
    ///     seek, which changes nothing. A cancel after it returns at once, and the seek ends in the background: its
    ///     writes complete, and if they succeed, the reader moves. The seeks of one partition run one at a time, in
    ///     the order they start, and the close waits for them.
    /// </summary>
    /// <exception cref="IggyInvalidStatusCodeException">
    ///     Status 3 for a partition that the reader does not read, and for a continuation of another partition.
    /// </exception>
    /// <exception cref="VsrRequestOutcomeUnknownException">A store or delete of the seek has an unknown outcome.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="token" /> was canceled.</exception>
    /// <exception cref="ObjectDisposedException">The reader is closed.</exception>
    public async Task SeekAsync(uint partitionId, PollingStrategy start, CancellationToken token = default)
    {
        start.ValidatePartition(partitionId);
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Task[] earlier;
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_closed, this);
            State(partitionId).HeldAt = _claims;
            earlier = [.. _seeks.Where(seek => seek.Partition == partitionId).Select(seek => seek.Done)];
            _seeks.Add((partitionId, done.Task));
        }

        var seek = RunSeekAsync(partitionId, start, earlier, done, token);
        try
        {
            await seek.WaitAsync(token);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            // The caller left, so only the log tells of a failure after the first write.
            _ = seek.ContinueWith(failed => _logger.LogWarning(failed.Exception,
                    "A canceled seek of partition {Partition} failed after its first write.", partitionId),
                CancellationToken.None, TaskContinuationOptions.OnlyOnFaulted, TaskScheduler.Default);
            throw;
        }
    }

    /// <summary>
    ///     Runs a seek after the earlier seeks of its partition and then ends its hold. The token of the caller stops it
    ///     only before its first write, so every write ends with its outcome while the hold stays.
    /// </summary>
    private async Task RunSeekAsync(uint partitionId, PollingStrategy start, Task[] earlier, TaskCompletionSource done,
        CancellationToken token)
    {
        try
        {
            await Task.WhenAll(earlier).WaitAsync(token);
            await SeekOnceAsync(partitionId, start, token);
        }
        finally
        {
            lock (_gate)
            {
                _seeks.Remove((partitionId, done.Task));
            }

            done.SetResult();
            Wake();
        }
    }

    private async Task SeekOnceAsync(uint partitionId, PollingStrategy start, CancellationToken token)
    {
        start = WithoutZeroContext(start);
        if (start.Kind == MessagePolling.First)
        {
            start = start with { Kind = MessagePolling.Offset, Value = 0 };
        }

        if (OnRead && start.Kind == MessagePolling.Timestamp)
        {
            // A read from the timestamp would go on from the stored offset, which may lie past the timestamp. Without a
            // message at the timestamp, every later message lies past the stored offset.
            var probe = await _client.PollMessagesAsync(_settings.StreamId, _settings.TopicId, partitionId,
                _settings.Consumer, start, _settings.Options with
                {
                    Commit = CommitPolicy.Manual,
                    Count = 1,
                    MinCount = 1,
                    MaxWait = TimeSpan.Zero
                }, token);
            if (probe.Messages.Count > 0)
            {
                start = start with { Kind = MessagePolling.Offset, Value = probe.Messages[0].Header.Offset };
            }
        }

        Task reading;
        lock (_gate)
        {
            _ = State(partitionId);
            reading = _committing.TryGetValue(partitionId, out var reads) ? reads.Drained.Task : Task.CompletedTask;
        }

        // No read of the partition starts during the seek, so after these reads nothing moves the stored offset before
        // the seek does. A transport lane does not order the writes after the reads: HTTP has none, and the lookup of a
        // seek to offset 0 takes none.
        await reading.WaitAsync(token);
        (ulong Offset, PartitionContext Context)? stored = null;
        if (OnRead && start.Kind == MessagePolling.Offset)
        {
            PartitionContext history;
            bool pins;
            ulong generation;
            lock (_gate)
            {
                var state = State(partitionId);
                generation = state.Generation;
                history = Owned(state, start.Context ?? state.Handed?.Context ?? default);
                // A position that follows the stored offset names no offset, so after a write with an unknown outcome
                // only the offset from before the write tells where the next read starts.
                pins = state.StoredUnknown || state.Position is { Kind: MessagePolling.Next, Context: null };
            }

            // The close restores the offset from before a write with an unknown outcome, and no position tells it: a
            // start or a check poll moves the position without a store. A read never moves the offset back, so a seek
            // to offset 0 deletes the offset that it finds, or the reads from Next would go on from it.
            var before = await _client.GetOffsetAsync(_settings.Consumer, _settings.StreamId, _settings.TopicId,
                partitionId, token);
            try
            {
                if (start.Value > 0)
                {
                    stored = (start.Value - 1, history);
                    token.ThrowIfCancellationRequested();
                    await StoreAsync(partitionId, start.Value - 1, history, _lifetime.Token);
                }
                else if (before is not null)
                {
                    token.ThrowIfCancellationRequested();
                    await DeleteStoredAsync(partitionId, history, _lifetime.Token);
                }
            }
            catch (VsrRequestOutcomeUnknownException error)
            {
                // The client holds the write until an abandon, which keeps later writes of the partition out.
                await AbandonLostAsync(error);
                lock (_gate)
                {
                    // A partition that left the assignment and came back starts afresh, because another member can
                    // have moved its offset since.
                    if (_states.TryGetValue(partitionId, out var state) && state.Generation == generation)
                    {
                        var context = state.Position.Context;
                        if (pins)
                        {
                            Pin(state, before);
                        }

                        // The close stores the offset from before the write, or deletes it when none existed. A read
                        // since the last batch can have stored from an earlier offset, which then stays the point. So
                        // does the point of an earlier write with an unknown outcome, which can have moved the offset
                        // that this lookup found.
                        var restore = before is { } found ? found.StoredOffset + 1 : 0;
                        if (!state.RestoresExactly && (state.PendingStart is not { } pending || pending.Value > restore))
                        {
                            state.PendingStart = context is { } known
                                ? PollingStrategy.Continuation(partitionId, MessagePolling.Offset, restore, known)
                                : PollingStrategy.Offset(restore);
                        }

                        state.Lost = true;
                        state.RestoresExactly = true;
                    }
                }

                throw;
            }
        }

        lock (_gate)
        {
            var state = State(partitionId);
            state.Generation = ++_generations;
            _sought[partitionId] = state.Generation;
            state.Position = start with { PartitionId = partitionId };
            state.StoredUnknown = false;
            state.Stopped = null;
            state.Lasting = 0;
            state.RetryAt = 0;
            state.CatchUpUntil = null;
            state.CatchUpBatch = null;
            state.PendingStart = null;
            state.Lost = false;
            state.AloneUntil = null;
            state.Counted = null;
            state.CountedSinceStore = 0;
            state.Stored = null;
            state.Delivered = stored;
            state.Handed = stored;
            state.FirstHanded = null;
            state.RestoresExactly = false;
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
    }

    /// <summary>
    ///     Gives a position that follows the stored offset the offset that it followed before a seek write with an
    ///     unknown outcome, under the lock. A start without a stored offset stays the start.
    /// </summary>
    private static void Pin(PartitionState state, OffsetResponse? before)
    {
        if (before is { } found)
        {
            state.Position = PollingStrategy.Offset(found.StoredOffset + 1);
        }
        else if (!state.StoredUnknown)
        {
            state.Position = PollingStrategy.Offset(0);
        }

        state.StoredUnknown = false;
    }

    /// <summary>
    ///     A start that the caller gives, with a fully zero context counted as none. A position from a reply keeps
    ///     a zero context, because under OnRead a Next position without one still needs its start poll.
    /// </summary>
    private static PollingStrategy WithoutZeroContext(PollingStrategy start)
    {
        return start.Context == default(PartitionContext) ? start with { Context = null } : start;
    }

    /// <summary>
    ///     Stops the reader. It waits for the reads in flight. With <see cref="CommitMode.Manual" /> it commits
    ///     nothing. With <see cref="CommitMode.Auto" />, it counts the last batch under
    ///     <see cref="CommitAfter.Processing" /> and stores the latest counted position of each partition. With
    ///     <see cref="CommitMode.OnRead" />, it moves the offset of each partition with undelivered batches back to
    ///     its last delivered message. A partition that delivered nothing gets the offset before its first undelivered
    ///     message, or no offset at offset 0, where it deletes only an offset that exists. If a store or delete has an
    ///     unknown outcome, the close fails. The reader then releases it in the client, so a later reader of the
    ///     partition can write. A store or delete that the server refuses as not owned reads its partition for the
    ///     installed owner. That read goes again after each transient refusal until <paramref name="token" /> is
    ///     canceled or 10 seconds after the close started. Then the refusal stands, and each later store or delete of
    ///     the close goes once without that read. The same read for an automatic store, for a seek in the background
    ///     or for a <see cref="CommitAsync" /> of the application ends when the close starts, and the close does not
    ///     wait for that commit. A group reader then leaves its group, also after a cancel. A
    ///     <see cref="NextAsync" /> that waits ends with <see cref="ObjectDisposedException" />.
    /// </summary>
    /// <remarks>
    ///     The close belongs to its first call. A later call waits for the first close and ends as it does, and its
    ///     token does not end that close. The stores and deletes of the close go one at a time, and the request
    ///     timeout of the client bounds each one. So during an outage a close can take about one request timeout for
    ///     each partition with a pending store.
    /// </remarks>
    /// <exception cref="Exception">The first failed store of the close.</exception>
    public async ValueTask CloseAsync(CancellationToken token)
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
                // The reads that the close cancels queue nothing, so only this ends a NextAsync without a token.
                _arrival.TrySetResult();
            }

            closed = _closing;
        }

        if (closing is not null)
        {
            try
            {
                await CloseOnceAsync(token);
                closing.SetResult();
            }
            catch (Exception error)
            {
                closing.SetException(error);
            }
        }

        await closed;
    }

    /// <summary>Stops the reader as <see cref="CloseAsync(CancellationToken)" /> does, with a token that is never canceled.</summary>
    public ValueTask CloseAsync()
    {
        return CloseAsync(CancellationToken.None);
    }

    private async Task CloseOnceAsync(CancellationToken token)
    {
        using var retry = CancellationTokenSource.CreateLinkedTokenSource(token);
        retry.CancelAfter(CloseOwnerReadBudget);
        CountUncounted();
        await _lifetime.CancelAsync();
        await _worker;
        await _refreshing;
        await _regrouping;
        await _lookingUp;
        Task[] reads;
        lock (_gate)
        {
            reads = [.. _reads];
        }

        await Task.WhenAll(reads);
        Task[] seeks;
        lock (_gate)
        {
            seeks = [.. _seeks.Select(seek => seek.Done)];
        }

        // A seek that waited for a read still applies, so the rewind sees its position and not the dropped batch.
        await Task.WhenAll(seeks);
        await _committer;
        Exception? failure = null;
        if (OnRead)
        {
            failure = await RewindAsync(retry.Token);
        }
        else if (AutoSettings is not null)
        {
            failure = await FlushCountedAsync(retry.Token);
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
                await AbandonLostAsync(error);
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

            if (await FlushCountedAsync(token) is { } error)
            {
                _logger.LogWarning(error, "The partition reader could not store a counted position.");
            }
        }
    }

    /// <summary>
    ///     Stores the latest counted position of each partition ahead of its last store, one at a time. The reader
    ///     releases a store with an unknown outcome in the client, so the next store of its partition is not blocked.
    /// </summary>
    private async Task<Exception?> FlushCountedAsync(CancellationToken retry)
    {
        List<(Checkpoint Counted, Checkpoint Owned)> due;
        lock (_gate)
        {
            due =
            [
                .. _states.Values
                    .Where(state => state.Counted is { } counted && counted.Stamp?.Generation == state.Generation
                                                                && (state.Stored is null
                                                                    || state.Stored.Offset < counted.Offset))
                    .Select(state => (state.Counted!, Owned(state, state.Counted!)))
            ];
        }

        Exception? failure = null;
        foreach (var (checkpoint, owned) in due)
        {
            try
            {
                await StoreAsync(owned, CancellationToken.None, retry);
                RecordStored(checkpoint);
            }
            catch (Exception error)
            {
                await AbandonLostAsync(error);
                failure ??= error;
            }
        }

        return failure;
    }

    /// <summary>
    ///     With <see cref="CommitMode.OnRead" /> the server stored the offsets of batches that the application never
    ///     took, so each partition with one moves its offset back.
    /// </summary>
    private async Task<Exception?> RewindAsync(CancellationToken retry)
    {
        List<(uint Partition, ulong? Offset, PartitionContext Context, bool Exact)> rewinds = [];
        lock (_gate)
        {
            var undelivered = new Dictionary<uint, PolledMessages>();
            foreach (var entry in _queue)
            {
                // A batch of an earlier assignment names an owner that the server replaced, and another member can have
                // moved the offset since, so it decides nothing.
                if (entry.Batch is { } batch && _states.TryGetValue(batch.PartitionId, out var owner)
                    && entry.Generation == owner.Generation)
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
                    rewinds.Add((partition, delivered.Offset, Owned(state, delivered.Context), true));
                }
                else
                {
                    var seen = state.FirstHanded ?? queued?.Messages[0].Header.Offset;
                    // A lost read stored from its start, which lies before the batch that the next read held back.
                    var first = Math.Min(seen ?? ulong.MaxValue, state.PendingStart?.Value ?? ulong.MaxValue);
                    ulong? offset = first == 0 ? null : first - 1;
                    var history = state.Handed?.Context ?? queued?.Context ?? state.PendingStart!.Value.Context;
                    rewinds.Add((partition, offset, Owned(state, history ?? default),
                        seen is not null || state.RestoresExactly));
                }
            }
        }

        Exception? failure = null;
        foreach (var (partition, offset, context, exact) in rewinds)
        {
            try
            {
                // An offset before a message that the reader saw exists. A seek write with an unknown outcome can have
                // moved the offset back or deleted it, so its rewind point is written as it is. Otherwise the read from
                // the start may have stored nothing.
                if (exact && offset is { } stored)
                {
                    await StoreAsync(partition, stored, context, retry);
                }
                else
                {
                    await MoveBackAsync(partition, offset, context, retry);
                }
            }
            catch (Exception error)
            {
                // The client holds a write with an unknown outcome until an abandon, which would keep the writes of a
                // later reader of the partition on this client out.
                await AbandonLostAsync(error);
                failure ??= error;
            }
        }

        return failure;
    }

    /// <summary>
    ///     Moves the stored offset of a partition back to the offset, or deletes it without one. A missing stored
    ///     offset, or one at or before the offset, stays, because no read stored past it. The server refuses the
    ///     delete of a missing offset and the store of an offset past the end.
    /// </summary>
    private async Task MoveBackAsync(uint partition, ulong? offset, PartitionContext context, CancellationToken retry)
    {
        if (await _client.GetOffsetAsync(_settings.Consumer, _settings.StreamId, _settings.TopicId, partition)
                is not { } found || offset >= found.StoredOffset)
        {
            return;
        }

        if (offset is { } point)
        {
            await StoreAsync(partition, point, context, retry);
        }
        else
        {
            await DeleteStoredAsync(partition, context, retry);
        }
    }

    /// <summary>Deletes the stored offset that a lookup found. A refusal of the delete as missing leaves no offset either.</summary>
    private async Task DeleteStoredAsync(uint partition, PartitionContext context, CancellationToken retry)
    {
        try
        {
            if (_fetcher is not null)
            {
                await WriteAsync(partition, context, owned => _client.DeleteOffsetAsync(_settings.Consumer,
                    _settings.StreamId, _settings.TopicId, partition, owned), retry);
            }
            else
            {
                await _client.DeleteOffsetAsync(_settings.Consumer, _settings.StreamId, _settings.TopicId, partition);
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
            try
            {
                TimeSpan wait;
                try
                {
                    ForgetEarlierSessionRefusals();
                    wait = StartReads();
                }
                catch (Exception error)
                {
                    // A worker that ended here would stop every read without an error, and the close awaits it.
                    Report(null, 0, error);
                    await Task.Delay(_settings.RetryInterval, token);
                    continue;
                }

                await _wake.WaitAsync(wait, token);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    /// <summary>
    ///     Starts the refusal count and the retry wait of every partition again at a new session, as the Rust reader
    ///     does at each new epoch: a refusal of an earlier session says nothing about the new one.
    /// </summary>
    private void ForgetEarlierSessionRefusals()
    {
        var session = Session;
        if (session == _refusalSession)
        {
            return;
        }

        _refusalSession = session;
        ForgetRefusals();
    }

    /// <summary>Starts the refusal count and the retry wait of every partition again.</summary>
    private void ForgetRefusals()
    {
        lock (_gate)
        {
            foreach (var state in _states.Values)
            {
                state.Lasting = 0;
                state.RetryAt = 0;
            }
        }
    }

    /// <summary>
    ///     Refreshes the assignment in a task of its own. A failed refresh keeps the partitions and is retried after
    ///     the retry interval, also when a caller asks again before.
    /// </summary>
    private async Task RefreshAsync()
    {
        var token = _lifetime.Token;
        try
        {
            await RefreshAssignmentAsync(token);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
        }
        catch (Exception error)
        {
            lock (_gate)
            {
                // The sync that a refusal as not owned asked for is still owed.
                _resync = true;
                _refreshAt = Environment.TickCount64 + (long)_settings.RetryInterval.TotalMilliseconds;
            }

            if (IsTransient(error) || IsMemberLost(error))
            {
                _logger.LogWarning(error, "The partition reader retries a refused refresh.");
            }
            else
            {
                Report(null, 0, error);
            }
        }
    }

    private async Task RefreshAssignmentAsync(CancellationToken token)
    {
        var session = Session;
        bool join;
        lock (_gate)
        {
            join = _memberLost || session != _joinedSession;
        }

        if (join)
        {
            await JoinGroupAsync(_client, _logger, _settings.StreamId, _settings.TopicId,
                _settings.Consumer.ConsumerId, token);
        }

        lock (_gate)
        {
            if (join)
            {
                _joinedSession = session;
                _memberLost = false;
                _syncedAt = null;
            }

            _resync = false;
        }

        IReadOnlyList<uint> assigned;
        try
        {
            // The worker starts a refresh only when a sync is due, so the client always asks the coordinator.
            assigned = await _assignment!.GetAssignedPartitionsAsync(_settings.StreamId, _settings.TopicId,
                _settings.Consumer.ConsumerId, true, token);
        }
        catch (Exception error) when (IsMemberLost(error))
        {
            // The partitions stay, as after a new session, and a refusal so far says nothing about the next member.
            lock (_gate)
            {
                _memberLost = true;
            }

            ForgetRefusals();
            throw;
        }

        List<Checkpoint> leaving = [];
        lock (_gate)
        {
            var owned = assigned.ToHashSet();
            // A join hands the partitions over one at a time, so after a join or a change the member asks again soon.
            // Only its own partitions count: hand-overs between other members move the group generation too.
            var changed = _syncedAt is null || !owned.SetEquals(_states.Keys);
            _regroupDue |= changed;
            _syncPause = changed
                ? AssignmentSettlePause
                : TimeSpan.FromTicks(Math.Min(_syncPause.Ticks * 2, GroupRefreshInterval.Ticks));
            _syncedAt = Environment.TickCount64;

            var left = _states.Keys.Where(partition => !owned.Contains(partition)).ToList();
            foreach (var partition in left)
            {
                var state = _states[partition];
                if (state.Counted is { } counted && (state.Stored is null || state.Stored.Offset < counted.Offset))
                {
                    leaving.Add(Owned(state, counted));
                }

                GiveUpGap(partition, state, LossCause.Moved);
                _states.Remove(partition);
            }

            Ungroup(left);

            foreach (var partition in owned.Where(partition => !_states.ContainsKey(partition)))
            {
                _states[partition] = NewState();
            }
        }

        foreach (var checkpoint in leaving)
        {
            try
            {
                await StoreAsync(checkpoint, token, token);
            }
            catch (Exception error) when (error is not OperationCanceledException)
            {
                await AbandonLostAsync(error);
                _logger.LogWarning(error, "Partition {Partition} left the reader before its counted position was stored.",
                    checkpoint.PartitionId);
            }
        }
    }

    /// <summary>
    ///     Starts the reads that may start now, at most <see cref="MaxRequestsInFlight" /> in flight, and returns how
    ///     long the worker may sleep. Over TCP the partitions of one primary share one request, and the fetches start
    ///     before the polls. A partition reads with its own poll for a start a request of several partitions cannot
    ///     take, while it reads again what a lost read stored, and while it reads a section that did not decode one
    ///     message at a time. The groups come from the last lookup, which holds every assigned partition, so the polls
    ///     leave one request for each group also while none of its partitions can fetch. A partition in no group yet,
    ///     new in the assignment or refused by its old primary, waits for the lookup in flight, as the Rust reader
    ///     holds a new assignment until its refresh ends, and reads with its own poll while lookups fail.
    /// </summary>
    private TimeSpan StartReads()
    {
        var now = Environment.TickCount64;
        var session = Session;
        var nextRetry = long.MaxValue;
        var refresh = false;
        List<uint>? regroup = null;
        IReadOnlyList<IReadOnlyList<uint>> groups;
        HashSet<uint> grouped = [];
        List<uint> alone = [];
        List<Target> lookups = [];
        int aloneLimit;
        lock (_gate)
        {
            // A refresh runs apart from the reads, so a later one that fails or gets no reply holds no read. After a
            // new session or a lost membership nothing else starts until a refresh confirmed the assignment, as in
            // the Rust reader.
            var synced = _memberLost || session != _joinedSession ? null : _syncedAt;
            var confirmed = _assignment is null || synced is not null;
            if (_assignment is not null && _refreshing.IsCompleted)
            {
                var due = Math.Max(_refreshAt,
                    synced is { } last && !_resync ? last + (long)_syncPause.TotalMilliseconds : now);
                refresh = due <= now;
                if (!refresh)
                {
                    nextRetry = due;
                }
            }

            if (confirmed && _fetcher is not null && _regroupDue && _regrouping.IsCompleted)
            {
                if (_regroupAt <= now)
                {
                    regroup = [.. _states.Keys];
                    _regroupDue = false;
                }
                else
                {
                    nextRetry = Math.Min(nextRetry, _regroupAt);
                }
            }

            groups = _groups;
            aloneLimit = Math.Max(MaxRequestsInFlight - groups.Count, 1);
            // A start lookup takes a request as a poll does. One pass looks up at a time, so its partitions settle at
            // once and their first reads share the request of their primary.
            var lookupRoom = _lookingUp.IsCompleted
                ? Math.Min(aloneLimit - Polling, MaxRequestsInFlight - Requests)
                : 0;
            var reading = confirmed && (OnRead
                ? _demand && _queue.Count == 0
                : _queuedMessages < _prefetchMessages && _queuedBytes < _prefetchBytes);
            if (reading)
            {
                foreach (var (partition, state) in _states)
                {
                    if (state.InFlight || state.Stopped is not null || Seeking(partition))
                    {
                        continue;
                    }

                    if (state.RetryAt > now)
                    {
                        nextRetry = Math.Min(nextRetry, state.RetryAt);
                        continue;
                    }

                    if (state.StoredUnknown)
                    {
                        if (lookups.Count < lookupRoom && Claim(partition) is { } lookup)
                        {
                            lookups.Add(lookup);
                        }

                        continue;
                    }

                    var inGroup = _grouped.Contains(partition);
                    if (!inGroup && _fetcher is not null && !_regroupFailed)
                    {
                        // A lookup is in flight or starts below, and its end wakes the worker.
                        continue;
                    }

                    if (inGroup && Fetches(state))
                    {
                        grouped.Add(partition);
                    }
                    else
                    {
                        alone.Add(partition);
                    }
                }
            }

            _lookups += lookups.Count;
        }

        if (refresh)
        {
            // The wake after its end starts the reads of a changed assignment and times the next refresh.
            _refreshing = RefreshAsync();
            _refreshing.ContinueWith(_ => Wake(), TaskScheduler.Default);
        }

        if (regroup is not null)
        {
            // Only a wake after the end of the task lets the next pass start a lookup that came due during this one.
            _regrouping = RegroupAsync(regroup);
            _regrouping.ContinueWith(_ => Wake(), TaskScheduler.Default);
        }

        if (lookups.Count > 0)
        {
            _lookingUp = LookUpStoredAsync(lookups);
            _lookingUp.ContinueWith(_ => Wake(), TaskScheduler.Default);
        }

        foreach (var fetch in Spread(groups, grouped, aloneLimit, alone.Count))
        {
            StartFetch(fetch);
        }

        StartPolls(alone, aloneLimit);

        return nextRetry == long.MaxValue
            ? Timeout.InfiniteTimeSpan
            : TimeSpan.FromMilliseconds(Math.Max(1, nextRetry - Environment.TickCount64));
    }

    /// <summary>
    ///     Looks up the groups of <paramref name="partitions" /> apart from the reads, as the Rust reader refreshes
    ///     them. A failed lookup keeps the groups it would replace and is retried after the retry interval, doubled
    ///     for each lasting refusal in a row.
    /// </summary>
    private async Task RegroupAsync(List<uint> partitions)
    {
        var token = _lifetime.Token;
        try
        {
            var groups = await _fetcher!.ReadinessGroupsAsync(_settings.StreamId, _settings.TopicId, partitions, token);
            lock (_gate)
            {
                _regroupRefusals = 0;
                // A change during the lookup may leave a partition with its old route in the reply, so the next
                // lookup replaces it.
                if (!_regroupDue)
                {
                    _groups = groups;
                    _grouped.Clear();
                    _grouped.UnionWith(groups.SelectMany(group => group));
                    _regroupFailed = false;
                }
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
        }
        catch (Exception error)
        {
            lock (_gate)
            {
                var now = Environment.TickCount64;
                var delay = (long)_settings.RetryInterval.TotalMilliseconds;
                if (error is IggyInvalidStatusCodeException status && IsLasting(status.StatusCode))
                {
                    _regroupRefusals++;
                    delay <<= Math.Min(_regroupRefusals - 1, MaxRegroupBackoffSteps);
                }

                _regroupDue = true;
                _regroupAt = now + delay;
                _regroupFailed = true;
            }

            if (IsTransient(error))
            {
                _logger.LogWarning(error, "The partition reader retries a failed lookup of its fetch groups.");
            }
            else
            {
                Report(null, 0, error);
            }
        }
    }

    /// <summary>Takes partitions out of their groups, under the lock, until a lookup groups them again.</summary>
    private void Ungroup(IEnumerable<uint> partitions)
    {
        var count = _grouped.Count;
        _grouped.ExceptWith(partitions);
        if (_grouped.Count < count)
        {
            _groups = [.. _groups.Select(group => (IReadOnlyList<uint>)[.. group.Where(_grouped.Contains)])
                .Where(group => group.Count > 0)];
        }
    }

    /// <summary>
    ///     Plans one fetch for each group with idle partitions while requests are free, in turn from the group after
    ///     the last one that fetched, so the groups left out go first in the next turn. The requests and the budget
    ///     room left after those fetches and the polls that wait deal the backlogged partitions of each group over
    ///     more fetches, one with the full limits each, so every group reads once before any group reads again. The
    ///     quiet partitions of a group share one fetch, which may wait on the server. With
    ///     <see cref="CommitMode.OnRead" /> a turn starts only while <see cref="NextAsync" /> waits, and a partition
    ///     is in one committing read at a time, so the fetches of a turn commit disjoint partitions.
    /// </summary>
    private List<IReadOnlyList<uint>> Spread(IReadOnlyList<IReadOnlyList<uint>> groups, IReadOnlySet<uint> idle,
        int aloneLimit, int alone)
    {
        List<(List<uint> Work, List<uint> Backlogged, List<uint> Quiet)> split = [];
        int free;
        lock (_gate)
        {
            var room = MaxRequestsInFlight - Requests;
            var start = _groupCursor;
            for (var index = 0; index < groups.Count && split.Count < room; index++)
            {
                var group = (start + index) % groups.Count;
                // A fetch reads its targets in order until it holds its count, so the partitions served last go last.
                var work = groups[group].Where(idle.Contains)
                    .OrderBy(partition => _states.TryGetValue(partition, out var state) ? state.ServedAt : 0).ToList();
                if (work.Count == 0)
                {
                    continue;
                }

                split.Add((work,
                    work.Where(partition => _states.TryGetValue(partition, out var state) && state.Backlogged).ToList(),
                    work.Where(partition => !_states.TryGetValue(partition, out var state) || !state.Backlogged).ToList()));
                _groupCursor = (group + 1) % groups.Count;
            }

            // The budget room counts full batches beside the queued batches and the requests in flight.
            var budgetRoom = Math.Min((_prefetchMessages - _queuedMessages) / _settings.Options.Count,
                (_prefetchBytes - _queuedBytes) / _settings.Options.MaxBytes) - Requests;
            free = (int)Math.Min(room, budgetRoom) - split.Count - Math.Min(alone, Math.Max(aloneLimit - Polling, 0));
        }

        // Every fetch holds at least one partition, so a group without quiet partitions keeps one backlogged.
        var extras = new int[split.Count];
        for (var dealt = true; dealt && free > 0;)
        {
            dealt = false;
            for (var index = 0; index < split.Count && free > 0; index++)
            {
                var (_, backlogged, quiet) = split[index];
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
            var (work, backlogged, quiet) = split[index];
            if (extras[index] == 0)
            {
                fetches.Add(work);
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

    /// <summary>
    ///     Polls the partitions that read alone in turn from the one after the last polled one, so a poll that ends
    ///     empty does not take the turn of the others.
    /// </summary>
    private void StartPolls(List<uint> alone, int limit)
    {
        var start = _pollCursor is { } cursor ? Math.Max(alone.FindIndex(partition => partition > cursor), 0) : 0;
        for (var index = 0; index < alone.Count; index++)
        {
            lock (_gate)
            {
                if (Polling >= limit || Requests >= MaxRequestsInFlight)
                {
                    return;
                }
            }

            var partition = alone[(start + index) % alone.Count];
            if (StartPoll(partition))
            {
                _pollCursor = partition;
            }
        }
    }

    private bool StartPoll(uint partition)
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
                return false;
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
            if (commits)
            {
                CountCommitting(partition);
                state.PendingStart ??= target.Sent;
            }
        }

        Track(PollAsync(target, commits, single, check, catchUpUntil), commits ? [partition] : []);
        return true;
    }

    private void StartFetch(IReadOnlyList<uint> group)
    {
        List<Target> targets = [];
        lock (_gate)
        {
            foreach (var partition in group)
            {
                // A seek that started and ended since the plan can leave a position that no fetch takes.
                if (_states.TryGetValue(partition, out var state) && Fetches(state) && Claim(partition) is { } target)
                {
                    targets.Add(target);
                    if (OnRead)
                    {
                        CountCommitting(partition);
                        state.PendingStart ??= target.Sent;
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

            Track(FetchAsync(targets), OnRead ? targets.ConvertAll(target => target.Partition) : []);
        }
    }

    /// <summary>
    ///     Whether a fetch can read the partition from its position, under the lock. The server takes only an offset or
    ///     Next in a fetch, and the other reads need a poll of their own.
    /// </summary>
    private bool Fetches(PartitionState state)
    {
        return state.Position.Kind is MessagePolling.Offset or MessagePolling.Next && state.CatchUpUntil is null
                                                                                  && state.AloneUntil is null
                                                                                  && !NeedsStart(state.Position);
    }

    /// <summary>Counts a committing read of the partition until its task ends, under the lock.</summary>
    private void CountCommitting(uint partition)
    {
        if (!_committing.TryGetValue(partition, out var reads))
        {
            reads = new CommittingReads();
            _committing[partition] = reads;
        }

        reads.Count++;
    }

    /// <summary>
    ///     Looks up the stored offsets of partitions that took a start that applies only without one. A partition with
    ///     a stored offset reads as with a Next start, and a failed lookup is refused as a failed read is.
    /// </summary>
    private async Task LookUpStoredAsync(List<Target> targets)
    {
        var lookups = targets.ConvertAll(async target => await _client.GetOffsetAsync(_settings.Consumer,
            _settings.StreamId, _settings.TopicId, target.Partition, _lifetime.Token));
        List<(Target Target, bool Stored)> found = [];
        List<(Target Target, Exception Error)> failed = [];
        foreach (var (target, lookup) in targets.Zip(lookups))
        {
            try
            {
                found.Add((target, await lookup is not null));
            }
            catch (Exception error)
            {
                failed.Add((target, error));
            }
        }

        lock (_gate)
        {
            _lookups -= targets.Count;
            foreach (var (target, stored) in found)
            {
                if (Settle(target) is not { } state || !Applies(state, target))
                {
                    continue;
                }

                state.StoredUnknown = false;
                if (stored)
                {
                    state.Position = PollingStrategy.Next();
                }
            }
        }

        foreach (var (target, error) in failed)
        {
            Refuse(target, error, false);
        }
    }

    /// <summary>Marks an idle partition as reading, under the lock.</summary>
    private Target? Claim(uint partition)
    {
        if (!_states.TryGetValue(partition, out var state) || state.InFlight || state.Stopped is not null
            || Seeking(partition))
        {
            return null;
        }

        state.InFlight = true;
        state.Claim = ++_claims;
        // The server serves a read with owner generation 0 for the owner installed for this client, at any metadata op.
        var session = Session;
        var wire = state.Position.Context is { } context && Replaces(state, context)
            ? state.Position with { Context = context with { OwnerGeneration = 0 } }
            : state.Position;
        return new Target(partition, state.Position, state.Generation, state.Claim, session, wire);
    }

    /// <summary>Whether a seek of the partition is in progress, under the lock.</summary>
    private bool Seeking(uint partition)
    {
        foreach (var seek in _seeks)
        {
            if (seek.Partition == partition)
            {
                return true;
            }
        }

        return false;
    }

    private void Track(Task read, List<uint> committing)
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
                foreach (var partition in committing)
                {
                    var reads = _committing[partition];
                    if (--reads.Count == 0)
                    {
                        _committing.Remove(partition);
                        reads.Drained.SetResult();
                    }
                }
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
                targets.ConvertAll(target => (target.Partition, target.Wire)), _settings.Consumer,
                _settings.Options.Count, OnRead, _settings.Options.Wire, _lifetime.Token);
        }
        catch (Exception error)
        {
            await AbandonLostAsync(error);
            foreach (var target in targets)
            {
                Refuse(target, error, OnRead);
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
                        Refuse(target, error!, OnRead);
                    }
                    else if (polled is null)
                    {
                        ReadAlone(target, section, error!);
                    }
                    else
                    {
                        Complete(target, polled, OnRead, section.NextOffset);
                    }
                }
            }
        }
    }

    /// <summary>
    ///     Whether a position first takes a poll that neither commits nor delivers, which moves it to its first
    ///     message. With <see cref="CommitMode.OnRead" /> a committing read starts only from an offset that it names, so
    ///     the close knows where to move the offset back when the read is lost.
    /// </summary>
    private bool NeedsStart(PollingStrategy position)
    {
        return OnRead && (position.Kind is MessagePolling.Last or MessagePolling.Timestamp
                          || position is { Kind: MessagePolling.Next, Context: null });
    }

    private async Task PollAsync(Target target, bool commits, bool single, bool check, ulong? catchUpUntil)
    {
        // A Last poll asks for the count of the reader, so the reads from its first message deliver the last count
        // messages.
        var one = single || check && target.Sent.Kind != MessagePolling.Last;
        var options = _settings.Options with
        {
            Commit = commits ? CommitPolicy.OnRead : CommitPolicy.Manual,
            Count = one ? 1 : _settings.Options.Count,
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
                _settings.Consumer, target.Wire, options, _lifetime.Token);
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
        catch (Exception error) when (check && target.Sent.Kind == MessagePolling.Next && _fetcher is not null
                                      && error is MalformedResponseException)
        {
            await StartAtUndecodedAsync(target, options.Count, error);
            return;
        }
        catch (Exception error)
        {
            await AbandonLostAsync(error);
            Refuse(target, error, commits);
            return;
        }

        if (check)
        {
            lock (_gate)
            {
                if (Settle(target) is { } state && Applies(state, target))
                {
                    Saw(state, target, polled.Context);
                    if (polled.Messages.Count > 0)
                    {
                        state.Position = PollingStrategy.Continuation(target.Partition,
                            target.Sent.Kind == MessagePolling.Next ? MessagePolling.Next : MessagePolling.Offset,
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

        Complete(target, polled, commits);
    }

    /// <summary>
    ///     A start read from Next whose reply does not decode tells no offset. A fetch section carries its offsets
    ///     apart from its records, so the reader sends the same read as a fetch without a commit and moves the
    ///     position as a start read that decodes does. The reads from there take the batch alone, so a message that
    ///     does not decode gets a skip. A fetch reads only from an offset or Next, so a timestamp or Last start keeps
    ///     the refusal.
    /// </summary>
    private async Task StartAtUndecodedAsync(Target target, uint count, Exception error)
    {
        FetchedSection? section;
        try
        {
            using var reply = await _fetcher!.FetchMessagesAsync(_settings.StreamId, _settings.TopicId,
                [(target.Partition, target.Wire)], _settings.Consumer, count, false, _settings.Options.Wire,
                _lifetime.Token);
            section = reply.Sections.FirstOrDefault(found => found.PartitionId == target.Partition);
        }
        catch (Exception fetchError)
        {
            await AbandonLostAsync(fetchError);
            Refuse(target, fetchError, false);
            return;
        }

        if (section is not { Status: 0, MessagesCount: > 0 })
        {
            Refuse(target, section is { Status: not 0 } ? section.Refusal() : error, false);
            return;
        }

        var start = section.NextOffset - Math.Min(section.NextOffset, section.MessagesCount);
        lock (_gate)
        {
            if (Settle(target) is { } state && Applies(state, target))
            {
                Saw(state, target, section.Context);
                state.Position = PollingStrategy.Continuation(target.Partition, MessagePolling.Next, start,
                    section.Context);
            }
        }
    }

    /// <summary>
    ///     Abandons the operation of a request of this reader with an unknown outcome, so the client takes the reads
    ///     and writes of its partitions again. The next read finds what a lost read or write stored.
    /// </summary>
    private Task AbandonLostAsync(Exception error)
    {
        return AbandonLostAsync(_client, _logger, error);
    }

    /// <summary>
    ///     Abandons the operation of a request with an unknown outcome, so the client takes the writes of its lane
    ///     again. A group write blocks the metadata lane, which every metadata write of the client takes.
    /// </summary>
    internal static async Task AbandonLostAsync(IIggyClient client, ILogger logger, Exception error)
    {
        if (error is not VsrRequestOutcomeUnknownException { OperationId: { } operation })
        {
            return;
        }

        try
        {
            await client.AbandonOperationAsync(operation);
        }
        catch (Exception abandon)
        {
            logger.LogWarning(abandon, "The lost request {Operation} could not be abandoned.", operation);
        }
    }

    /// <summary>
    ///     Joins the consumer group. A join with an unknown outcome is abandoned at once, and the next attempt joins
    ///     again, which the server takes as a no-op when the lost join landed.
    /// </summary>
    internal static async Task JoinGroupAsync(IIggyClient client, ILogger logger, Identifier streamId,
        Identifier topicId, Identifier groupId, CancellationToken token)
    {
        try
        {
            await client.JoinConsumerGroupAsync(streamId, topicId, groupId, token);
        }
        catch (VsrRequestOutcomeUnknownException error)
        {
            await AbandonLostAsync(client, logger, error);
            throw;
        }
    }

    /// <summary>Hands a reply of a read to the application and moves the position of its partition after it.</summary>
    private void Complete(Target target, PolledMessages polled, bool commits, ulong? nextOffset = null)
    {
        lock (_gate)
        {
            if (Settle(target) is not { } state)
            {
                // A partition that left the assignment still hands over a read that the server stored. A read again of
                // a lost range does not: the leave reported that range as lost and queued the batch after it.
                if (commits && polled.Messages.Count > 0 && Keeps(target.Partition, target.Generation))
                {
                    EnqueueBatch(target, polled);
                }

                return;
            }

            state.Lasting = 0;
            Saw(state, target, polled.Context);
            // A fetch section that ended before the head of its partition reads again at once.
            state.Backlogged = nextOffset is { } sectionEnd && sectionEnd <= polled.CurrentOffset
                                                            && (polled.Messages.Count > 0 || polled.CurrentOffset > 0);
            if (polled.Messages.Count == 0)
            {
                var emptyNext = nextOffset ?? polled.CurrentOffset + 1;
                if (OnRead && state.Lost && target.Wire is { Kind: MessagePolling.Next, Context: { } context }
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
            if (OnRead && state.Lost && target.Wire is { Kind: MessagePolling.Next, Context: { } sentContext }
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

            // A read below the first kept message starts at it, as after retention, so the gap before it is gone.
            if (state.CatchUpUntil is { } gapEnd && target.Wire is { Kind: MessagePolling.Offset, Context: { } gap }
                && first > target.Sent.Value && polled.Context != default && SameHistory(gap, polled.Context))
            {
                // The held batch keeps its messages, so only the offsets before it can be gone.
                if (first >= (state.CatchUpBatch?.Messages[0].Header.Offset ?? gapEnd + 1))
                {
                    GiveUpGap(target.Partition, state, LossCause.Gone);
                }
                else
                {
                    Enqueue(target.Partition, target.Generation,
                        LostRange(target.Partition, target.Sent.Value, first - 1, LossCause.Gone));
                }

                // The reply is dropped: it can run past the gap into offsets that no read stored. Nothing before its
                // first message is left, so the position moves up to that message, and a Next from there skips nothing.
                if (first > state.Position.Value)
                {
                    state.Position = PollingStrategy.Continuation(target.Partition, state.Position.Kind, first,
                        polled.Context);
                }

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
    ///     A read that did not return its partition. A refusal before the read retries, and only a lasting, a
    ///     history or a tiered read refusal stops the partition until a seek.
    /// </summary>
    private void Refuse(Target target, Exception error, bool commits)
    {
        ReadException? report = null;
        lock (_gate)
        {
            if (error is IggyInvalidStatusCodeException
                {
                    StatusCode: VsrError.NOT_PRIMARY or VsrError.HISTORY_UNAVAILABLE
                    or VsrError.CONSUMER_GROUP_PARTITION_NOT_OWNED
                })
            {
                // The client dropped the route, so a fetch of the old group would look it up and hold the other
                // partitions of the fetch while it waits.
                Ungroup([target.Partition]);
                _regroupDue = true;
            }

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
                    // A refusal alone is not a leave, and the refresh drops a partition that left, so the close still
                    // needs what the partition holds. The server can have installed the owner again with a new
                    // generation, which the next reply shows. So the partition never stops, and each sixth refusal in
                    // a row reports one error.
                    state.Unowned = true;
                    state.LostOwner = Math.Max(state.LostOwner,
                        Math.Max(state.SeenOwner, state.Position.Context?.OwnerGeneration ?? 0));
                    _resync = true;
                    state.Lasting++;
                    state.RetryAt = now + interval * (1L << (state.Lasting - 1));
                    if (state.Lasting < MaxLastingRefusals)
                    {
                        return;
                    }

                    state.Lasting = 0;
                    report = new ReadException(target.Partition, error);
                    break;
                case IggyInvalidStatusCodeException { StatusCode: VsrError.HISTORY_UNAVAILABLE }
                    when (target.Sent.Context ?? default) == default:
                    _logger.LogWarning(
                        "Partition {Partition} was refused with a history that the reader never read. It reads again.",
                        target.Partition);
                    state.RetryAt = now + interval;
                    return;
                case IggyInvalidStatusCodeException
                {
                    StatusCode: VsrError.HISTORY_UNAVAILABLE or VsrError.INVALID_SIZE_BYTES or VsrError.TIERED_READ_FAILED
                } refusal:
                    GiveUpGap(target.Partition, state, LossCause.Stopped);
                    report = StopReport(target.Partition, state, error,
                        refusal.StatusCode != VsrError.INVALID_SIZE_BYTES);
                    state.Stopped = report;
                    break;
                case IggyInvalidStatusCodeException status when IsLasting(status.StatusCode):
                    state.Lasting++;
                    if (state.Lasting >= MaxLastingRefusals)
                    {
                        GiveUpGap(target.Partition, state, LossCause.Stopped);
                        report = StopReport(target.Partition, state, error, true);
                        state.Stopped = report;
                        break;
                    }

                    _logger.LogWarning(error, "Partition {Partition} was refused, it reads again later.",
                        target.Partition);
                    state.RetryAt = now + interval * (1L << (state.Lasting - 1));
                    return;
                default:
                    // The client can lose the connection or the session after the server stored a committing read, for
                    // example when the reply comes after the session ended, so the next read finds what it stored.
                    state.Lost |= commits && IsReaderWide(error);
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

    /// <summary>
    ///     The stop of a partition. A committing read that lost its reply stays pending until the next read shows what
    ///     it stored, and a seek to Next after the stop would skip that range without a word, so the stop reports the
    ///     lost read instead of its own error. A catch-up that <see cref="GiveUpGap" /> ended reported that range
    ///     before, so the stop then reports its own error.
    /// </summary>
    private static ReadException StopReport(uint partition, PartitionState state, Exception error, bool resumable)
    {
        var reported = state.Lost
            ? VsrError.Exception(VsrError.FETCH_PROGRESS_UNCERTAIN,
                $"A lost read may have stored offsets of partition {partition} from {state.Position.Value}, and the " +
                $"partition stopped on: {error.Message}")
            : error;
        return new ReadException(partition, reported) { StopsPartition = true, IsResumable = resumable };
    }

    /// <summary>
    ///     Ends the read again of a gap: reports the offsets before the held batch with their
    ///     <paramref name="cause" />, and queues that batch.
    /// </summary>
    private void GiveUpGap(uint partition, PartitionState state, LossCause cause)
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
            Enqueue(partition, state.Generation, LostRange(partition, state.Position.Value, first - 1, cause));
        }

        var next = held is null ? until + 1 : held.Messages[^1].Header.Offset + 1;
        if (held is not null)
        {
            held.Messages = held.Messages.Where(message => message.Header.Offset >= state.Position.Value).ToArray();
        }

        state.CatchUpUntil = null;
        state.CatchUpBatch = null;
        // The catch-up accounted for what the lost read stored, so a stop that follows reports its own error.
        state.Lost = false;
        state.Position = PollingStrategy.Continuation(partition, MessagePolling.Next, next,
            held?.Context ?? state.Position.Context!.Value);
        if (held is { Messages.Count: > 0 })
        {
            EnqueueBatch(target, held);
        }
    }

    /// <summary>The report of offsets that a lost read stored and that the reader does not read again.</summary>
    private static ReadException LostRange(uint partition, ulong from, ulong until, LossCause cause)
    {
        return new ReadException(partition, VsrError.Exception(VsrError.FETCH_PROGRESS_UNCERTAIN,
            $"A lost read stored offsets {from} to {until} of partition {partition}, and " + cause switch
            {
                LossCause.Gone => "its history no longer holds them.",
                LossCause.Stopped => "the partition stopped before they were read again.",
                _ => "the partition moved before they were read again."
            }))
        {
            Lost = new OffsetRange(from, until)
        };
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
        return state.Generation == target.Generation ? state : null;
    }

    /// <summary>
    ///     Whether the result of a check or a lookup still applies to its partition, under the lock. A seek owns the
    ///     position while it runs, and the server can answer a check or a lookup after the write of the seek landed. So
    ///     only a check or a lookup claimed after the last seek began applies, and only to the position that it read.
    /// </summary>
    private static bool Applies(PartitionState state, Target target)
    {
        return target.Claim > state.HeldAt && state.Position.Equals(target.Sent);
    }

    /// <summary>
    ///     Whether a request of the partition replaces the group owner of the context with 0, under the lock. After a
    ///     session change the server installs the owner again with a new generation and refuses the old one, and a not
    ///     owned refusal can mean the same. So the owner of a context from an earlier session, an owner that a refused
    ///     read lost, and the owner of any context until the next reply after a session change or a not owned refusal
    ///     are replaced. Any other context of the current session goes out as it is, also one of an older owner, which
    ///     the server refuses.
    /// </summary>
    private bool Replaces(PartitionState state, PartitionContext context)
    {
        return context.OwnerGeneration != 0
               && (state.Unowned || context.OwnerGeneration <= state.LostOwner || state.Owner is not { } owner
                   || owner.Session != Session || context.OwnerGeneration < owner.Generation);
    }

    /// <summary>
    ///     The context of an offset write of the partition, under the lock. The server binds owner generation 0 to the
    ///     owner of this client and session only when that owner was installed at or before the metadata op of the
    ///     write, so a write that replaces the owner also carries the latest metadata op of the client.
    /// </summary>
    private PartitionContext Owned(PartitionState state, PartitionContext context)
    {
        return Replaces(state, context)
            ? context with { OwnerGeneration = 0, MetadataOp = Math.Max(context.MetadataOp, Watermark) }
            : context;
    }

    /// <summary>The checkpoint that a write of the partition carries, under the lock.</summary>
    private Checkpoint Owned(PartitionState state, Checkpoint checkpoint)
    {
        return new Checkpoint(checkpoint.PartitionId, checkpoint.Offset, Owned(state, checkpoint.Context),
            checkpoint.Stamp);
    }

    private Task StoreAsync(uint partition, ulong offset, PartitionContext context, CancellationToken retry)
    {
        return WriteAsync(partition, context, owned => _client.StoreOffsetAsync(_settings.Consumer, _settings.StreamId,
            _settings.TopicId, offset, partition, owned), retry);
    }

    private Task StoreAsync(Checkpoint checkpoint, CancellationToken token, CancellationToken retry)
    {
        return WriteAsync(checkpoint.PartitionId, checkpoint.Context, owned => _client.StoreConsumerPositionAsync(
            _settings.Consumer, _settings.StreamId, _settings.TopicId,
            new Checkpoint(checkpoint.PartitionId, checkpoint.Offset, owned, checkpoint.Stamp), token), retry);
    }

    /// <summary>
    ///     A source that <paramref name="token" /> cancels, and so does the start of the close. A store that went out
    ///     after the close could land after the leave. The close disposes the lifetime in the end, so a call after its
    ///     start links a canceled token instead.
    /// </summary>
    private CancellationTokenSource LinkToCloseStart(CancellationToken token)
    {
        lock (_gate)
        {
            return CancellationTokenSource.CreateLinkedTokenSource(token,
                _closed ? new CancellationToken(true) : _lifetime.Token);
        }
    }

    /// <summary>
    ///     Sends an offset write of the partition. A not owned refusal alone is not a leave: the server can have
    ///     installed the owner again, also at a metadata op that this client never saw. So while the partition stays
    ///     in the assignment, the reader treats its owner as lost, asks for the assignment again and reads the
    ///     partition once. When that read is served in the incarnation of the write, the write goes once more under the
    ///     context of the reply. Otherwise the refusal stands, also for a partition that left the assignment.
    ///     <paramref name="retry" /> ends the retries of that read. For a commit of the application it is the token of
    ///     the caller or the start of the close, whichever comes first. For a write in the background it is the
    ///     lifetime of the reader, which the close ends. For a write of the close it is the deadline of the close. A
    ///     cancel of it after the reply of that read also keeps the refusal.
    /// </summary>
    private async Task WriteAsync(uint partition, PartitionContext context, Func<PartitionContext, Task> write,
        CancellationToken retry)
    {
        try
        {
            await write(context);
        }
        catch (IggyInvalidStatusCodeException error)
            when (error.StatusCode == VsrError.CONSUMER_GROUP_PARTITION_NOT_OWNED)
        {
            lock (_gate)
            {
                if (!_states.TryGetValue(partition, out var state))
                {
                    throw;
                }

                state.Unowned = true;
                _resync = true;
            }

            Wake();
            if (await ProbeOwnerAsync(partition, context, retry) is not { } served || retry.IsCancellationRequested)
            {
                throw;
            }

            await write(served);
        }
    }

    /// <summary>
    ///     The context of one read of the partition without a commit and with owner generation 0, which the server
    ///     serves for the owner installed for this client. Only that context counts, so a reply whose message does not
    ///     decode serves as well. A transient refusal of the read makes it read again after the retry interval until
    ///     <paramref name="retry" /> is canceled. Null once the retry ends, after any other failure, also an unknown
    ///     outcome, or when the read serves another incarnation than the context names. Incarnation 0 names none.
    /// </summary>
    private async Task<PartitionContext?> ProbeOwnerAsync(uint partition, PartitionContext context,
        CancellationToken retry)
    {
        var pause = TimeSpan.Zero;
        while (true)
        {
            PartitionContext served;
            try
            {
                await Task.Delay(pause, retry);
                served = (await _client.PollMessagesAsync(_settings.StreamId, _settings.TopicId, partition,
                    _settings.Consumer,
                    PollingStrategy.Continuation(partition, MessagePolling.Next, 0,
                        context with { OwnerGeneration = 0 }),
                    _settings.Options with
                    {
                        Commit = CommitPolicy.Manual,
                        Count = 1,
                        MinCount = 1,
                        MaxWait = TimeSpan.Zero
                    }, retry)).Context;
            }
            catch (Exception error) when (IsTransientRefusal(error))
            {
                pause = _settings.RetryInterval;
                continue;
            }
            catch (Exception error) when (UndecodedReplyContext(error) is { } replied)
            {
                served = replied;
            }
            catch (Exception error)
            {
                await AbandonLostAsync(error);
                return null;
            }

            return served != default && (context.Incarnation == 0 || served.Incarnation == context.Incarnation)
                ? served
                : null;
        }
    }

    /// <summary>The context of a poll reply whose messages do not decode, as the reply carried it.</summary>
    private static PartitionContext? UndecodedReplyContext(Exception error)
    {
        return error switch
        {
            MalformedResponseException malformed => malformed.Context,
            MessageDecryptionException decryption => decryption.Context,
            _ => null
        };
    }

    /// <summary>
    ///     Records a reply of the partition under the lock. Owner generations only grow, so the first one that a
    ///     session shows bounds the contexts of that session from below. An empty reply records its owner as well.
    /// </summary>
    private static void Saw(PartitionState state, Target target, PartitionContext context)
    {
        if (context == default)
        {
            return;
        }

        state.Unowned = false;
        state.SeenOwner = Math.Max(state.SeenOwner, context.OwnerGeneration);
        if (state.Owner is not { } owner || owner.Session < target.Session)
        {
            state.Owner = (target.Session, context.OwnerGeneration);
        }
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

    private PartitionState NewState()
    {
        return new PartitionState(_settings.Start, ++_generations) { StoredUnknown = LooksUpStart };
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

    /// <summary>
    ///     Whether a reply comes from the history of the sent position. The server binds a sent owner generation 0 to
    ///     the owner that it installed, also again after a session change.
    /// </summary>
    private static bool SameHistory(PartitionContext sent, PartitionContext served)
    {
        return sent.Incarnation == served.Incarnation
               && (sent.OwnerGeneration == 0 || sent.OwnerGeneration == served.OwnerGeneration);
    }

    private static bool IsTransient(Exception error)
    {
        return IsTransientRefusal(error) || error is VsrRequestOutcomeUnknownException;
    }

    /// <summary>A refusal of the server that a retry soon after can pass.</summary>
    private static bool IsTransientRefusal(Exception error)
    {
        return error is IggyInvalidStatusCodeException
        {
            StatusCode: VsrError.TRANSIENT_NOT_ACCEPTED or VsrError.TRANSIENT_NOT_COMMITTED or VsrError.NOT_PRIMARY
            or VsrError.DEFERRED_POLL_CAPACITY_EXCEEDED or VsrError.LIFECYCLE_BUSY
        };
    }

    /// <summary>
    ///     A sync that found no member of the client in the group, for example after the group was deleted and created
    ///     again. The reader joins again by itself, so it reports no error for it.
    /// </summary>
    private static bool IsMemberLost(Exception error)
    {
        return error is IggyInvalidStatusCodeException { StatusCode: VsrError.CONSUMER_GROUP_MEMBER_NOT_FOUND };
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

    /// <summary>
    ///     A read of one partition. Its claim tells it from a later read of the partition, and zero reads nothing. Its
    ///     session is the one of the client at the claim. The reader sends its wire position: the position with the
    ///     owner generation that a write of that session carries.
    /// </summary>
    private readonly record struct Target(
        uint Partition,
        PollingStrategy Sent,
        ulong Generation,
        ulong Claim = 0,
        ulong Session = 0,
        PollingStrategy Wire = default);

    /// <summary>A batch or an error, with the generation of its partition when the reader read it.</summary>
    private sealed record Entry(
        uint? PartitionId,
        ulong Generation,
        PolledMessages? Batch,
        ReadException? Error,
        long Messages,
        long Bytes);

    /// <summary>Why the reader could not read again the offsets that a lost read stored.</summary>
    private enum LossCause
    {
        /// <summary>The partition left the assignment of the reader.</summary>
        Moved,

        /// <summary>The history of the partition no longer holds the offsets.</summary>
        Gone,

        /// <summary>The partition stopped until a seek.</summary>
        Stopped
    }

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

        /// <summary>
        ///     The claim count when the last seek of the partition began. A check or a lookup claimed before it can show
        ///     the stored offset that the seek wrote, so the reader drops its result.
        /// </summary>
        internal ulong HeldAt;

        internal bool InFlight;
        internal int Lasting;

        /// <summary>With an on read policy: a committing read of the partition lost its reply and may have stored.</summary>
        internal bool Lost;

        /// <summary>
        ///     The latest session that a reply of the partition came from, and the first owner generation of that
        ///     session.
        /// </summary>
        internal (ulong Session, ulong Generation)? Owner;

        /// <summary>The highest owner generation that a reply of the partition showed.</summary>
        internal ulong SeenOwner;

        /// <summary>
        ///     A read refused as not owned loses the owner of the position and every owner seen before it for good, so
        ///     no request of the partition names an owner generation up to this one.
        /// </summary>
        internal ulong LostOwner;

        /// <summary>
        ///     A read was refused as not owned, so until the next reply the partition sends owner generation 0.
        /// </summary>
        internal bool Unowned;

        /// <summary>
        ///     A seek write of this assignment had an unknown outcome. The close writes its rewind point without a lookup,
        ///     because that write can have moved the stored offset back.
        /// </summary>
        internal bool RestoresExactly;

        internal PollingStrategy Position = start;
        internal long RetryAt;

        /// <summary>When a read last served messages of the partition, so served partitions go last in a fetch.</summary>
        internal ulong ServedAt;

        internal ReadException? Stopped;
        internal Checkpoint? Stored;

        /// <summary>
        ///     The partition took a start that applies only without a stored offset, and no lookup found one yet.
        /// </summary>
        internal bool StoredUnknown;
    }

    /// <summary>The committing reads in flight of one partition, and the end of the last one.</summary>
    private sealed class CommittingReads
    {
        internal readonly TaskCompletionSource Drained = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal int Count;
    }
}
