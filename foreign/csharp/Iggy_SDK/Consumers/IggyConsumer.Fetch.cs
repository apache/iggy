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

using System.Runtime.ExceptionServices;
using Apache.Iggy.Contracts;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Kinds;
using Apache.Iggy.Mappers;
using Apache.Iggy.Vsr;
using Microsoft.Extensions.Logging;

namespace Apache.Iggy.Consumers;

public partial class IggyConsumer
{
    /// <summary>
    ///     The fetches in flight. No two name the same partition. Only a poll under <see cref="_pollingSemaphore" />
    ///     changes the list, and <see cref="DisposeAsync" /> takes the semaphore before it ends them.
    /// </summary>
    private readonly List<GroupFetch> _fetches = [];

    /// <summary>When each partition last got messages. A fetch names the partitions served longest ago first.</summary>
    private readonly Dictionary<uint, long> _servedAt = [];

    /// <summary>
    ///     The first and the last offset that committing fetches committed in the current history of each partition.
    ///     At dispose they show the committed messages that the application never took.
    /// </summary>
    private readonly Dictionary<uint, (ulong First, ulong Last, PartitionContext Context)> _committedOffsets = [];

    private long _servedClock;
    private bool _resyncAssignment;
    private readonly HashSet<uint> _blockedFetchPartitions = [];
    private Exception? _fetchRecoveryFailure;

    /// <summary>
    ///     The assignment of a group member whose client fetches several partitions in one request (code 109), such
    ///     as TCP. Without one the member keeps the deferred group poll without a partition.
    /// </summary>
    private IConsumerGroupAssignmentProvider? AssignmentProvider =>
        _config.Consumer.Type == ConsumerType.ConsumerGroup ? _client as IConsumerGroupAssignmentProvider : null;

    /// <summary>
    ///     The poll of one partition that this call runs, or null when the call delivered or failed a fetch, or found
    ///     no partition. A consumer without fetches always polls <see cref="NextPoll" />. A fetching consumer polls
    ///     one partition only for an oversized record, and for a partition without a position whose configured
    ///     strategy a fetch cannot start.
    /// </summary>
    private async Task<PollPlan?> NextPollAsync(bool rented, CancellationToken ct)
    {
        if (AssignmentProvider is not { } assignment)
        {
            return NextPoll();
        }

        if (IsDisposed)
        {
            return null;
        }
        if (_fetchRecoveryFailure is { } recovery)
        {
            ExceptionDispatchInfo.Throw(recovery);
        }

        var replied = _fetches.Find(fetch => fetch.Reply.IsCompleted);
        if (replied is null)
        {
            if (_oversizedPartition is not null)
            {
                return NextPoll();
            }

            using var polling = CancellationTokenSource.CreateLinkedTokenSource(ct, _lifetime.Token);
            if (await StartFetchesAsync(assignment, polling.Token) is { } start)
            {
                return new PollPlan(start, _config.BatchSize, _config.PollOptions, false);
            }

            if (_fetches.Count == 0)
            {
                // Partitions come back under a new owner generation, which refuses a kept continuation.
                _nextPollingStrategies.Clear();
                LogNoPartitionAssignedBackingOff(NoAssignedPartitionBackoffMs);
                await Task.Delay(NoAssignedPartitionBackoffMs, polling.Token);
                return null;
            }

            await Task.WhenAny(_fetches.Select(fetch => fetch.Reply)).WaitAsync(polling.Token);
            replied = _fetches.First(fetch => fetch.Reply.IsCompleted);
        }

        await DeliverFetchAsync(replied, rented);
        return null;
    }

    /// <summary>
    ///     Starts a fetch for each readiness group of the assignment that needs one, and returns a partition that a
    ///     fetch cannot start: one without a position whose configured strategy is not Offset or Next. A committing
    ///     consumer keeps one fetch per group, sent only while the application waits for messages, because each
    ///     committed fetch that the application did not take is lost in a crash. A partition that joins its group
    ///     during a park then waits for the next fetch of the group. Without commits, a joining partition goes into
    ///     a second fetch, and it joins the main fetch after that one replies. A fetch that names a partition the
    ///     member no longer owns is canceled, unless it commits. A partition whose record waits to be read alone stays
    ///     out of the fetches until that poll, which would otherwise refuse it again.
    /// </summary>
    private async Task<uint?> StartFetchesAsync(IConsumerGroupAssignmentProvider assignment, CancellationToken token)
    {
        var assigned = await assignment.GetAssignedPartitionsAsync(_config.StreamId, _config.TopicId,
            _config.Consumer.ConsumerId, _resyncAssignment, token);
        _resyncAssignment = false;
        var owned = assigned.ToHashSet();
        foreach (var fetch in _fetches)
        {
            if (fetch.Cancellation is { } cancellation && !fetch.Partitions.All(owned.Contains))
            {
                cancellation.Cancel();
            }
        }

        foreach (var partition in _nextPollingStrategies.Keys)
        {
            if (!owned.Contains(partition))
            {
                _nextPollingStrategies.TryRemove(partition, out _);
            }
        }

        uint? start = null;
        var fetchable = new List<uint>(assigned.Count);
        foreach (var partition in assigned.OrderBy(partition => _servedAt.GetValueOrDefault(partition)))
        {
            if (partition == _oversizedPartition)
            {
                continue;
            }

            if (_nextPollingStrategies.ContainsKey(partition)
                || _config.PollingStrategy.Kind is MessagePolling.Offset or MessagePolling.Next)
            {
                fetchable.Add(partition);
            }
            else
            {
                start ??= partition;
            }
        }

        if (fetchable.Count == 0)
        {
            return start;
        }

        var groups = await _client.ReadinessGroupsAsync(_config.StreamId, _config.TopicId, fetchable, token);
        var busy = _fetches.SelectMany(fetch => fetch.Partitions.Concat(fetch.Held)).ToHashSet();
        foreach (var group in groups)
        {
            var members = group.ToHashSet();
            var main = _fetches.Find(fetch => fetch.Main is null && fetch.Partitions.Any(members.Contains));
            if (main is not null && (_config.AutoCommit
                                     || _fetches.Exists(fetch => fetch.Main is not null
                                                                 && fetch.Partitions.Any(members.Contains))))
            {
                continue;
            }

            var idle = group.Where(partition => !busy.Contains(partition)).ToList();
            if (idle.Count > 0)
            {
                StartFetch(idle, main);
            }
        }

        return start;
    }

    private void StartFetch(List<uint> partitions, GroupFetch? main)
    {
        var targets = partitions.ConvertAll(partition => (partition, PollingStrategyFor(partition)));
        var commits = _config.AutoCommit;
        var cancellation = commits ? null : CancellationTokenSource.CreateLinkedTokenSource(_lifetime.Token);
        // A committing fetch commits what it returns, so its count bounds the loss of a crash. Any other fetch fills
        // the receive buffer, which is empty whenever a reply is delivered.
        var reply = _client.FetchMessagesAsync(_config.StreamId, _config.TopicId, targets, _config.Consumer,
            commits ? _config.BatchSize : _config.BatchSize * ReceiveBufferBatches, commits, _config.PollOptions,
            cancellation?.Token ?? _lifetime.Token);
        _fetches.Add(new GroupFetch(partitions, main, reply, cancellation));
    }

    /// <summary>
    ///     Takes the reply of a fetch: moves each partition to the next offset of its section, starts the next
    ///     fetches when the fetch does not commit, and then delivers the messages. With an encryptor the messages
    ///     are decrypted first, and a section that does not decrypt keeps its position, so the next fetch reads it
    ///     again. A decode failure also discards that partition from fetches started before the failure. A refusal
    ///     of the whole fetch with a lost history or ownership does not name its partition, so every
    ///     target starts over from the configured strategy, and the duplicate filter drops what was delivered.
    /// </summary>
    private async Task DeliverFetchAsync(GroupFetch fetch, bool rented)
    {
        _fetches.Remove(fetch);
        var canceled = fetch.Cancellation?.IsCancellationRequested == true;
        fetch.Cancellation?.Dispose();
        FetchedMessages reply;
        try
        {
            reply = await fetch.Reply;
        }
        catch (Exception error) when (RequiresFetchRecovery(error))
        {
            RecordFetchRecovery(error, fetch.Partitions);
            throw;
        }
        catch (Exception) when (canceled || IsDisposed)
        {
            return;
        }
        catch (IggyInvalidStatusCodeException error) when (error.FromServer
            && error.StatusCode is VsrError.HISTORY_UNAVAILABLE or VsrError.CONSUMER_GROUP_PARTITION_NOT_OWNED)
        {
            foreach (var partition in fetch.Partitions)
            {
                _nextPollingStrategies.TryRemove(partition, out _);
            }

            _resyncAssignment |= error.StatusCode == VsrError.CONSUMER_GROUP_PARTITION_NOT_OWNED;
            throw;
        }

        using (reply)
        {
            var sections = reply.Sections;
            var decrypted = new PolledMessagesRental?[sections.Count];
            var encrypted = _client.MessageEncryptor is not null;
            Exception? sectionFailure = null;
            try
            {
                for (var index = 0; index < sections.Count; index++)
                {
                    var section = sections[index];
                    if (fetch.Discarded.Contains(section.PartitionId))
                    {
                        continue;
                    }

                    if (section.Status != 0)
                    {
                        if (section.ProgressError is { } recovery)
                        {
                            RecordFetchRecovery(recovery, [section.PartitionId]);
                            sectionFailure ??= recovery;
                            continue;
                        }
                        RefuseSection(section);
                        continue;
                    }

                    if (encrypted && section.MessagesCount > 0)
                    {
                        try
                        {
                            decrypted[index] = section.DecodeRented();
                        }
                        catch (Exception error) when (error is MessageDecryptionException or MalformedResponseException)
                        {
                            sectionFailure ??= error;
                            RewindSection(section);
                            continue;
                        }
                    }

                    AdvanceSection(section);
                }

                if (fetch.Main is { } main && _fetches.Contains(main))
                {
                    main.Held.AddRange(fetch.Partitions);
                }

                Exception? startFailure = null;
                if (!_config.AutoCommit)
                {
                    try
                    {
                        await StartFetchesAsync((IConsumerGroupAssignmentProvider)_client, _lifetime.Token);
                    }
                    catch (Exception error) when (!IsDisposed)
                    {
                        // The positions moved already, so the messages are delivered before the failure is reported.
                        startFailure = error;
                    }
                }

                for (var index = 0; index < sections.Count; index++)
                {
                    var section = sections[index];
                    if (section is not { Status: 0, MessagesCount: > 0 }
                        || fetch.Discarded.Contains(section.PartitionId) || encrypted && decrypted[index] is null)
                    {
                        continue;
                    }

                    PolledMessagesRental rental;
                    try
                    {
                        rental = decrypted[index] ?? section.DecodeRented();
                    }
                    catch (Exception error) when (error is MessageDecryptionException or MalformedResponseException)
                    {
                        sectionFailure ??= error;
                        RewindSection(section);
                        continue;
                    }

                    decrypted[index] = null;
                    // The delivery ignores the caller's token: the positions moved, so a canceled delivery would
                    // skip the messages.
                    if (rented)
                    {
                        await PublishRentedBatchAsync(rental, _lifetime.Token);
                    }
                    else
                    {
                        using (rental)
                        {
                            await PublishPolledAsync(BinaryMapper.MaterializeMessages(rental), _lifetime.Token);
                        }
                    }
                }

                if (sectionFailure is not null && _fetchRecoveryFailure is null)
                {
                    ExceptionDispatchInfo.Throw(sectionFailure);
                }

                if (startFailure is not null)
                {
                    ExceptionDispatchInfo.Throw(startFailure);
                }
            }
            finally
            {
                foreach (var rental in decrypted)
                {
                    rental?.Dispose();
                }
            }
        }
    }

    private void RewindSection(FetchedSection section)
    {
        var start = section.NextOffset - Math.Min(section.NextOffset, section.MessagesCount);
        _nextPollingStrategies[section.PartitionId] = PollingStrategy.Offset(start).WithContext(section.Context);
        foreach (var pending in _fetches)
        {
            if (pending.Partitions.Contains(section.PartitionId))
            {
                pending.Discarded.Add(section.PartitionId);
            }
        }
    }

    /// <summary>
    ///     Sends the partition as <see cref="MessagePolling.Offset" /> of the next offset from now on, also after a
    ///     <see cref="MessagePolling.Next" /> section without messages, and moves a served partition to the end.
    /// </summary>
    private void AdvanceSection(FetchedSection section)
    {
        var partition = section.PartitionId;
        _nextPollingStrategies[partition] = PollingStrategy.Offset(section.NextOffset).WithContext(section.Context);
        if (section.MessagesCount == 0)
        {
            return;
        }

        _servedAt[partition] = ++_servedClock;
        if (_config.AutoCommit)
        {
            RecordCommitted(section);
        }
    }

    /// <summary>The offsets of one partition are dense, so a section holds the messages up to its next offset.</summary>
    private void RecordCommitted(FetchedSection section)
    {
        var partition = section.PartitionId;
        var last = section.NextOffset - 1;
        _committedOffsets[partition]
            = _committedOffsets.TryGetValue(partition, out var committed)
              && SamePartition(committed.Context, section.Context)
                ? (committed.First, last, section.Context)
                : (section.NextOffset - section.MessagesCount, last, section.Context);
    }

    /// <summary>
    ///     A refused section keeps the position of its partition. Not primary needs nothing more: the client marked
    ///     the route moved, so the next grouping looks it up again. An oversized record is read alone by the next
    ///     poll. Any other refusal reaches the error events, as a poll's does, and a lost history or ownership also
    ///     starts the partition over from the configured strategy.
    /// </summary>
    private void RefuseSection(FetchedSection section)
    {
        switch ((int)section.Status)
        {
            case VsrError.NOT_PRIMARY:
                return;
            case VsrError.INVALID_SIZE_BYTES:
                _oversizedPartition ??= section.PartitionId;
                return;
            case VsrError.HISTORY_UNAVAILABLE or VsrError.CONSUMER_GROUP_PARTITION_NOT_OWNED:
                _nextPollingStrategies.TryRemove(section.PartitionId, out _);
                _resyncAssignment |= section.Status == VsrError.CONSUMER_GROUP_PARTITION_NOT_OWNED;
                break;
        }

        var error = section.Refusal();
        LogFailedToPollMessages(error);
        _consumerErrorEvents.Publish(new ConsumerErrorEventArgs(error, "Failed to poll messages"));
    }

    /// <summary>
    ///     Ends the fetches in flight at dispose, after <see cref="_lifetime" /> canceled each one that does not
    ///     commit. A committing fetch can commit before a cancel arrives, so it is never canceled: dispose waits for
    ///     its reply, at most its request timeout. Its messages never reach the application, so their offsets join
    ///     <see cref="_committedOffsets" />.
    /// </summary>
    private async Task FinishFetchesAsync()
    {
        await _pollingSemaphore.WaitAsync();
        try
        {
            foreach (var fetch in _fetches)
            {
                try
                {
                    using var reply = await fetch.Reply;
                    if (fetch.Cancellation is not null)
                    {
                        continue;
                    }

                    foreach (var section in reply.Sections)
                    {
                        if (section.ProgressError is { } recovery)
                        {
                            RecordFetchRecovery(recovery, [section.PartitionId]);
                        }
                        if (section is { Status: 0, MessagesCount: > 0 })
                        {
                            RecordCommitted(section);
                        }
                    }
                }
                catch (Exception error) when (RequiresFetchRecovery(error))
                {
                    RecordFetchRecovery(error, fetch.Partitions);
                }
                catch (Exception error) when (fetch.Cancellation is null && error is not OperationCanceledException)
                {
                    _logger.LogWarning(error, "A committing fetch of partitions {Partitions} ended without a reply",
                        string.Join(", ", fetch.Partitions));
                }
                catch (Exception)
                {
                    // A canceled fetch was not sent or commits nothing, so it holds nothing to keep.
                }
                finally
                {
                    fetch.Cancellation?.Dispose();
                }
            }

            _fetches.Clear();
        }
        finally
        {
            _pollingSemaphore.Release();
        }
    }

    /// <summary>
    ///     The server committed messages that the application never took. With a delivered message of the same
    ///     partition history, the stored offset moves back to that message. Without one, the consumer stores the
    ///     offset before the first committed message, or deletes it before offset zero. Both carry the
    ///     context of the committed messages, so neither lands in a newer history.
    /// </summary>
    private async Task RewindUndeliveredOffsetsAsync(CancellationToken token)
    {
        foreach (var (partition, committed) in _committedOffsets)
        {
            if (_blockedFetchPartitions.Contains(partition))
            {
                continue;
            }
            if (token.IsCancellationRequested)
            {
                break;
            }

            ulong? offset;
            if (_deliveredOffsets.TryGetValue(partition, out var delivered)
                && SamePartition(delivered.Context, committed.Context))
            {
                if (committed.Last <= delivered.Offset)
                {
                    continue;
                }

                offset = delivered.Offset;
            }
            else
            {
                offset = committed.First == 0 ? null : committed.First - 1;
            }

            try
            {
                await WithLaneRecoveryAsync(operation => offset is { } stored
                    ? _client.StoreOffsetAsync(_config.Consumer, _config.StreamId, _config.TopicId, stored, partition,
                        committed.Context, operation)
                    : _client.DeleteOffsetAsync(_config.Consumer, _config.StreamId, _config.TopicId, partition,
                        committed.Context, operation), token);
                if (offset is { } rewound)
                {
                    _storedOffsets[partition] = (rewound, committed.Context);
                }
                else
                {
                    _storedOffsets.Remove(partition);
                }
            }
            catch (Exception error)
            {
                if (RequiresFetchRecovery(error))
                {
                    RecordFetchRecovery(error, [partition]);
                }
                _logger.LogWarning(error, "Failed to rewind the offset before undelivered messages of partition {PartitionId}",
                    partition);
            }
        }
    }

    private static bool RequiresFetchRecovery(Exception error) =>
        error is VsrFetchProgressUncertainException or VsrLaneBlockedException { RequiresExplicitRecovery: true };

    private void RecordFetchRecovery(Exception error, IEnumerable<uint> partitions)
    {
        _fetchRecoveryFailure ??= error;
        if (error is VsrLaneBlockedException { PartitionId: { } partition })
        {
            _blockedFetchPartitions.Add(partition);
        }
        else
        {
            _blockedFetchPartitions.UnionWith(partitions);
        }
    }

    /// <summary>
    ///     A fetch in flight. A second fetch of a group names its <see cref="Main" /> fetch, which still parked when
    ///     it was sent. When the second one replies first, its partitions wait in <see cref="Held" /> of the main
    ///     fetch, so the next fetch of the group names them all. Only a committing fetch has no
    ///     <see cref="Cancellation" />.
    /// </summary>
    private sealed class GroupFetch(
        IReadOnlyList<uint> partitions,
        GroupFetch? main,
        Task<FetchedMessages> reply,
        CancellationTokenSource? cancellation)
    {
        public IReadOnlyList<uint> Partitions { get; } = partitions;
        public GroupFetch? Main { get; } = main;
        public Task<FetchedMessages> Reply { get; } = reply;
        public CancellationTokenSource? Cancellation { get; } = cancellation;
        public List<uint> Held { get; } = [];
        public HashSet<uint> Discarded { get; } = [];
    }
}
