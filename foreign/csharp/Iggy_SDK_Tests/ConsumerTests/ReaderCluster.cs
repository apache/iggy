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
using Apache.Iggy.IggyClient.Implementations;
using Apache.Iggy.Kinds;
using Apache.Iggy.Tests.VsrTests;
using Apache.Iggy.Utils;
using Apache.Iggy.Vsr;
using Microsoft.Extensions.Logging.Abstractions;
using static Apache.Iggy.Tests.VsrTests.MockFrames;

namespace Apache.Iggy.Tests.ConsumerTests;

/// <summary>A fetch as one node of the cluster received it.</summary>
internal readonly record struct ReceivedFetch(int Node, FetchRequest Request);

/// <summary>A deferred poll as the cluster received it.</summary>
internal readonly record struct ReceivedPoll(uint PartitionId, MessagePolling Kind, ulong Value, bool AutoCommit, uint Count);

/// <summary>
///     Nodes that each lead the partitions that <see cref="Primary" /> names. A partition holds the offsets 0 to
///     <see cref="Limit" /> minus 1, and a read serves two messages from its start. A message has its offset as its
///     timestamp, so a poll from a timestamp starts at the offset of that value. A read of a partition that
///     this node does not lead is refused as not primary. As on the server, a committing read moves the stored offset
///     only forward, a store sets it, a delete of a missing offset is refused, and a fetch with a target that starts
///     neither at an offset nor at Next is refused as an invalid command.
/// </summary>
internal sealed class ReaderCluster : IDisposable
{
    private const int PartitionsCount = 4;

    /// <summary>The context of every route and every read.</summary>
    internal static readonly PartitionContext Served = new(17, 9, 52);

    /// <summary>A StoreConsumerOffset body ends with the partition id, the offset and the ack quorum.</summary>
    private const int StoredPartitionFromEnd = sizeof(uint) + sizeof(ulong) + sizeof(byte);

    /// <summary>A DeleteConsumerOffset body ends with the partition id and the ack quorum.</summary>
    private const int DeletedPartitionFromEnd = sizeof(uint) + sizeof(byte);

    /// <summary>A GetConsumerOffset body ends with the partition id.</summary>
    private const int LookedUpPartitionFromEnd = sizeof(uint);

    /// <summary>A GetConsumerOffset reply holds the partition id, the head offset and the stored offset.</summary>
    private const int OffsetReplySize = sizeof(uint) + sizeof(ulong) + sizeof(ulong);

    /// <summary>The status of a delete of a missing offset.</summary>
    private const uint ConsumerOffsetNotFound = 3021;

    private readonly ConcurrentDictionary<uint, int> _moved = new();
    private readonly MockNode[] _nodes;
    private readonly ConcurrentDictionary<(int Node, UInt128 Client, ulong Request), MockRequest> _parked = new();
    private readonly ConcurrentQueue<(int Node, MockRequest Request)> _held = new();
    private readonly ConcurrentQueue<(int Node, MockRequest Request)> _heldPolls = new();
    private readonly ConcurrentQueue<(int Node, MockRequest Request)> _heldLookups = new();
    private readonly ConcurrentQueue<(int Node, MockRequest Request)> _heldStores = new();
    private readonly ConcurrentQueue<(int Node, MockRequest Request)> _heldLogouts = new();
    private uint[] _assignment = [];
    private ulong _groupGeneration = 1;
    private int _cancels;
    private int _joins;
    private int _leaves;
    private int _tieredReadRefusals;
    private int _syncs;
    private int _routeLookups;
    private int _refusedRouteLookups;

    internal ReaderCluster(int nodes)
    {
        _nodes = [.. Enumerable.Range(0, nodes).Select(_ => new MockNode())];
        for (var node = 0; node < nodes; node++)
        {
            var index = node;
            _nodes[node].Serve(request => Handle(index, request));
        }
    }

    internal ulong Limit { get; set; } = 2;

    /// <summary>
    ///     The owner generation of every partition. A read or an offset write that names another one is refused as not
    ///     owned.
    /// </summary>
    internal ulong OwnerGeneration { get; set; } = Served.OwnerGeneration;

    /// <summary>
    ///     The metadata op that installed the owner, which a join answers as its commit. An offset write of owner
    ///     generation 0 binds to the owner only when its metadata op is at or after it.
    /// </summary>
    internal ulong InstallOp { get; set; }

    /// <summary>Partitions whose committing fetch section is answered as not committed, so its outcome is unknown.</summary>
    internal ConcurrentBag<uint> UncertainSections { get; } = [];

    /// <summary>Partitions whose fetch is refused as not owned whatever it names.</summary>
    internal ConcurrentBag<uint> NotOwned { get; } = [];

    /// <summary>A poll that reads by offset without a commit gets no reply, so a recovery stays in flight.</summary>
    internal bool RecoveryUnanswered { get; set; }

    /// <summary>Fetches get no reply until <see cref="ReleaseHeldAsync" />, which serves them as they are then.</summary>
    internal bool HoldFetches { get; set; }

    /// <summary>Every offset store is refused with status 87.</summary>
    internal bool StoresRefused { get; set; }

    /// <summary>Every offset delete is refused with status 87.</summary>
    internal bool DeletesRefused { get; set; }

    /// <summary>The next offset store is applied and answered as too old, so the client cannot tell its outcome.</summary>
    internal bool LoseNextStoreReply { get; set; }

    /// <summary>The next offset store is refused as not owned, whatever owner it names.</summary>
    internal bool NextStoreNotOwned { get; set; }

    /// <summary>Every offset store is refused as not owned, whatever owner it names.</summary>
    internal bool StoresNotOwned { get; set; }

    /// <summary>The owner generations of the offset stores, as the cluster received them.</summary>
    internal ConcurrentQueue<ulong> StoreOwners { get; } = new();

    /// <summary>The metadata ops of the offset stores, as the cluster received them.</summary>
    internal ConcurrentQueue<ulong> StoreOps { get; } = new();

    /// <summary>The next offset delete is applied and answered as too old, so the client cannot tell its outcome.</summary>
    internal bool LoseNextDeleteReply { get; set; }

    /// <summary>The next group write of this kind is applied and answered as too old, so the client cannot tell its outcome.</summary>
    internal VsrOperation? LoseNextGroupReply { get; set; }

    /// <summary>Every offset lookup is refused with this status.</summary>
    internal int? LookupRefusal { get; set; }

    /// <summary>Every probe is refused with this status.</summary>
    internal int? ProbeRefusal { get; set; }

    /// <summary>
    ///     The probes the cluster received: reads without a commit of one message after the stored offset. A reader
    ///     sends one to find the owner of a write refused as not owned, and under OnRead to find its start.
    /// </summary>
    internal int Probes => Polls.Count(IsProbe);

    internal int HeldFetches => _held.Count;

    /// <summary>The fetches that wait for <see cref="ReleaseHeldAsync" />, as the nodes received them.</summary>
    internal IReadOnlyList<FetchRequest> Held => [.. _held.Select(held => ReadFetch(held.Request))];

    /// <summary>Polls get no reply until <see cref="ReleaseHeldPollsAsync" />, which serves them as they are then.</summary>
    internal bool HoldPolls { get; set; }

    /// <summary>The partitions of the polls that wait for <see cref="ReleaseHeldPollsAsync" />, as the nodes received them.</summary>
    internal IReadOnlyList<uint> HeldPolls => [.. _heldPolls.Select(held => ReadPoll(held.Request).PartitionId)];

    /// <summary>Offset lookups get no reply until <see cref="ReleaseHeldLookupsAsync" />.</summary>
    internal bool HoldLookups { get; set; }

    /// <summary>Offset lookups of this partition get no reply until <see cref="ReleaseHeldLookupsAsync" />.</summary>
    internal uint? HoldLookupsOf { get; set; }

    internal int HeldLookups => _heldLookups.Count;

    /// <summary>Offset stores get no reply until <see cref="ReleaseHeldStoresAsync" />, which serves them as they are then.</summary>
    internal bool HoldStores { get; set; }

    internal int HeldStores => _heldStores.Count;

    /// <summary>Sign-outs get no reply until <see cref="ReleaseHeldLogoutsAsync" />, so the session ended meanwhile.</summary>
    internal bool HoldLogouts { get; set; }

    internal int HeldLogouts => _heldLogouts.Count;

    /// <summary>A served section reports the head of its partition, as the server does, instead of its last offset.</summary>
    internal bool ReportsHead { get; init; }

    internal bool OversizedLast { get; init; }

    /// <summary>Reads carry a fully zero context, as from a server that stamps none. Routes keep theirs.</summary>
    internal bool ReadsUnstamped { get; init; }

    internal bool RecoveryNotOwned { get; set; }

    /// <summary>The next committing fetch or poll that serves messages stores them and loses its reply.</summary>
    internal bool LoseNextCommittingRead { get; set; }

    internal Action? OnLostCommittingRead { get; set; }

    /// <summary>The partitions of the member. A change moves the group to a new generation, as the server does.</summary>
    internal uint[] Assignment
    {
        get => _assignment;
        set
        {
            if (!value.SequenceEqual(_assignment))
            {
                Interlocked.Increment(ref _groupGeneration);
            }

            _assignment = value;
        }
    }

    /// <summary>The assignment that the member moves to once a committing fetch is served.</summary>
    internal uint[]? AssignmentAfterRead { get; set; }

    /// <summary>The assignment that the member moves to once a sync is served.</summary>
    internal uint[]? AssignmentAfterSync { get; set; }

    /// <summary>Each sync finds the group in a new generation, as hand-overs between other members leave it.</summary>
    internal bool RebalancesEachSync { get; init; }

    /// <summary>
    ///     The group holds no member of the client, as after a delete and a new create of the group. As on the server,
    ///     a sync then gets an empty body until the client joins again.
    /// </summary>
    internal bool MemberForgotten { get; set; }

    /// <summary>Every join is refused with this status, as for a group that stays deleted.</summary>
    internal int? JoinRefusal { get; set; }

    /// <summary>Every sync is refused with this status, as by a node that cannot answer it yet.</summary>
    internal int? SyncRefusal { get; set; }

    /// <summary>Syncs get no reply, as from a node that is cut off.</summary>
    internal bool SyncsUnanswered { get; set; }

    internal HashSet<ulong> Undecodable { get; } = [];

    internal ConcurrentBag<uint> HistoryRefused { get; } = [];

    /// <summary>Partitions whose fetch section is refused as a failed tiered read.</summary>
    internal ConcurrentBag<uint> TieredReadRefused { get; } = [];

    /// <summary>The fetch sections refused as a failed tiered read so far.</summary>
    internal int TieredReadRefusals => Volatile.Read(ref _tieredReadRefusals);

    /// <summary>Partitions whose fetch section is refused as unauthorized.</summary>
    internal ConcurrentBag<uint> Unauthorized { get; } = [];

    internal ConcurrentDictionary<uint, ulong> Stored { get; } = new();

    /// <summary>The partitions of the offset lookups, as the cluster received them.</summary>
    internal ConcurrentQueue<uint> Lookups { get; } = new();

    /// <summary>The partitions of the offset deletes, as the cluster received them.</summary>
    internal ConcurrentQueue<uint> Deletes { get; } = new();

    internal ConcurrentQueue<ReceivedFetch> Fetches { get; } = new();

    internal ConcurrentQueue<ReceivedPoll> Polls { get; } = new();

    internal int Cancels => Volatile.Read(ref _cancels);

    internal int Joins => Volatile.Read(ref _joins);

    internal int Leaves => Volatile.Read(ref _leaves);

    internal int Syncs => Volatile.Read(ref _syncs);

    /// <summary>Refuses every route lookup, so the reader cannot plan a read.</summary>
    internal bool RoutesRefused { get; set; }

    /// <summary>Partitions whose route lookup is refused with the status.</summary>
    internal ConcurrentDictionary<uint, int> RefusedRoutes { get; } = new();

    internal int RouteLookups => Volatile.Read(ref _routeLookups);

    internal int RefusedRouteLookups => Volatile.Read(ref _refusedRouteLookups);

    /// <summary>A fetch reads its targets in order until it holds its count, as the server does.</summary>
    internal bool CountLimited { get; init; }

    /// <summary>A fetch replies without sections.</summary>
    internal bool Sectionless { get; init; }

    public void Dispose()
    {
        foreach (var node in _nodes)
        {
            node.Dispose();
        }
    }

    /// <summary>Serves the <paramref name="count" /> oldest held fetches, or all of them.</summary>
    internal Task ReleaseHeldAsync(int count = int.MaxValue)
    {
        return ReleaseAsync(_held, count, Fetch);
    }

    /// <summary>Serves the <paramref name="count" /> oldest held polls, or all of them.</summary>
    internal Task ReleaseHeldPollsAsync(int count = int.MaxValue)
    {
        return ReleaseAsync(_heldPolls, count, Poll);
    }

    internal Task ReleaseHeldLookupsAsync()
    {
        return ReleaseAsync(_heldLookups, int.MaxValue, (_, request) => Offset(request));
    }

    internal Task ReleaseHeldStoresAsync()
    {
        return ReleaseAsync(_heldStores, int.MaxValue, (_, request) => Store(request));
    }

    internal Task ReleaseHeldLogoutsAsync()
    {
        return ReleaseAsync(_heldLogouts, int.MaxValue, (_, request) => Answer(request));
    }

    internal int Primary(uint partition)
    {
        return _moved.TryGetValue(partition, out var node) ? node : (int)(partition % (uint)_nodes.Length);
    }

    internal void Move(uint partition, int node)
    {
        _moved[partition] = node;
    }

    internal async Task<TcpMessageStream> ConnectAsync(IMessageEncryptor? encryptor = null)
    {
        var client = new TcpMessageStream(new IggyClientConfigurator
        {
            BaseAddress = $"127.0.0.1:{_nodes[0].Port}",
            Protocol = Protocol.Tcp,
            MessageEncryptor = encryptor
        }, NullLoggerFactory.Instance);
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        await client.LoginUserAsync("user", "secret", TestContext.Current.CancellationToken);
        return client;
    }

    private async Task ReleaseAsync(ConcurrentQueue<(int Node, MockRequest Request)> held, int count,
        Func<int, MockRequest, byte[]?> serve)
    {
        while (count-- > 0 && held.TryDequeue(out var read))
        {
            if (serve(read.Node, read.Request) is { } reply)
            {
                await _nodes[read.Node].AnswerParkedAsync(read.Request, reply);
            }
        }
    }

    private byte[]? Handle(int node, MockRequest request)
    {
        switch ((VsrOperation)request.Operation)
        {
            case VsrOperation.StoreConsumerOffset when HoldStores:
                _heldStores.Enqueue((node, request));
                return null;
            case VsrOperation.StoreConsumerOffset:
                return Store(request);
            case VsrOperation.DeleteConsumerOffset:
                var deleted = BinaryPrimitives.ReadUInt32LittleEndian(request.Body.AsSpan(^DeletedPartitionFromEnd));
                Deletes.Enqueue(deleted);
                if (RefusesHistory(request))
                {
                    return Reply(request.Operation, [], VsrError.HISTORY_UNAVAILABLE);
                }

                if (RefusesOwner(request))
                {
                    return Reply(request.Operation, [], VsrError.CONSUMER_GROUP_PARTITION_NOT_OWNED);
                }

                if (DeletesRefused)
                {
                    return Reply(request.Operation, [], VsrError.HISTORY_UNAVAILABLE);
                }

                if (!Stored.TryRemove(deleted, out _))
                {
                    return Reply(request.Operation, [], ConsumerOffsetNotFound);
                }

                if (LoseNextDeleteReply)
                {
                    LoseNextDeleteReply = false;
                    return Reply(request.Operation, [], VsrError.REQUEST_TOO_OLD);
                }

                return Answer(request);
            case VsrOperation.JoinConsumerGroup:
                Interlocked.Increment(ref _joins);
                if (JoinRefusal is { } refused)
                {
                    return Reply(request.Operation, [], (uint)refused);
                }

                MemberForgotten = false;
                return GroupReply(request);
            case VsrOperation.LeaveConsumerGroup:
                Interlocked.Increment(ref _leaves);
                return GroupReply(request);
            case VsrOperation.Logout when HoldLogouts:
                _heldLogouts.Enqueue((node, request));
                return null;
        }

        switch (request.Code)
        {
            case GET_CLUSTER_METADATA_CODE:
                return Roster();
            case CommandCodes.GET_PARTITION_ROUTE_CODE:
                Interlocked.Increment(ref _routeLookups);
                if (RoutesRefused)
                {
                    return Reply(request.Operation, [], VsrError.UNAUTHORIZED);
                }

                var routed = BinaryPrimitives.ReadUInt32LittleEndian(request.Body.AsSpan(^sizeof(uint)));
                if (RefusedRoutes.TryGetValue(routed, out var refusal))
                {
                    Interlocked.Increment(ref _refusedRouteLookups);
                    return Reply(request.Operation, [], (uint)refusal);
                }

                var primary = (byte)Primary(routed);
                return RouteReply(request, Context, primary, PartitionsCount, primary);
            case CommandCodes.GET_CONSUMER_OFFSET_CODE when HoldLookups || HoldLookupsOf == LookedUp(request):
                _heldLookups.Enqueue((node, request));
                return null;
            case CommandCodes.GET_CONSUMER_OFFSET_CODE:
                return Offset(request);
            case CommandCodes.SYNC_CONSUMER_GROUP_CODE:
                Interlocked.Increment(ref _syncs);
                if (SyncsUnanswered)
                {
                    return null;
                }

                if (SyncRefusal is { } unsynced)
                {
                    return Reply(OPERATION_NON_REPLICATED, [], (uint)unsynced);
                }

                if (MemberForgotten)
                {
                    return Reply(OPERATION_NON_REPLICATED, []);
                }

                if (RebalancesEachSync)
                {
                    Interlocked.Increment(ref _groupGeneration);
                }

                var synced = Reply(OPERATION_NON_REPLICATED,
                    AssignmentBody(Interlocked.Read(ref _groupGeneration), Assignment));
                if (AssignmentAfterSync is { } next)
                {
                    Assignment = next;
                    AssignmentAfterSync = null;
                }

                return synced;
            case CommandCodes.FETCH_MESSAGES_CODE when HoldFetches:
                _held.Enqueue((node, request));
                return null;
            case CommandCodes.FETCH_MESSAGES_CODE:
                return Fetch(node, request);
            case CommandCodes.POLL_MESSAGES_DEFERRED_CODE when HoldPolls:
                _heldPolls.Enqueue((node, request));
                return null;
            case CommandCodes.POLL_MESSAGES_DEFERRED_CODE:
                return Poll(node, request);
            case CommandCodes.CANCEL_DEFERRED_REQUEST_CODE:
                Interlocked.Increment(ref _cancels);
                if (_parked.TryRemove((node, request.ClientId, request.RequestId), out var parked))
                {
                    _ = Task.Run(() => _nodes[node].AnswerParkedAsync(parked,
                        Reply(parked.Operation, [], VsrError.TRANSIENT_NOT_ACCEPTED)));
                }

                return null;
            default:
                return Answer(request);
        }
    }

    private byte[]? Fetch(int node, MockRequest request)
    {
        var fetch = ReadFetch(request);
        Fetches.Enqueue(new ReceivedFetch(node, fetch));
        if (fetch.Targets.Any(target => target.Kind is not (MessagePolling.Offset or MessagePolling.Next)))
        {
            return Reply(request.Operation, [], VsrError.INVALID_COMMAND);
        }

        // The server fences the whole fetch, so the refusal names no target.
        if (fetch.Targets.Any(target => NotOwned.Contains(target.PartitionId)
                                        || target.OwnerGeneration != 0 && target.OwnerGeneration != OwnerGeneration))
        {
            return Reply(request.Operation, [], VsrError.CONSUMER_GROUP_PARTITION_NOT_OWNED);
        }

        List<FetchSection> sections = [];
        var served = false;
        var held = 0u;
        foreach (var target in fetch.Targets)
        {
            var partition = target.PartitionId;
            if (Primary(partition) != node)
            {
                sections.Add(EmptySection(partition, 0, default, VsrError.NOT_PRIMARY));
                continue;
            }

            if (fetch.AutoCommit && UncertainSections.Contains(partition))
            {
                sections.Add(EmptySection(partition, 0, default, VsrError.TRANSIENT_NOT_COMMITTED));
                served = true;
                continue;
            }

            if (HistoryRefused.Contains(partition))
            {
                sections.Add(EmptySection(partition, 0, default, VsrError.HISTORY_UNAVAILABLE));
                served = true;
                continue;
            }

            if (TieredReadRefused.Contains(partition))
            {
                Interlocked.Increment(ref _tieredReadRefusals);
                sections.Add(EmptySection(partition, 0, default, VsrError.TIERED_READ_FAILED));
                served = true;
                continue;
            }

            if (Unauthorized.Contains(partition))
            {
                sections.Add(EmptySection(partition, 0, default, VsrError.UNAUTHORIZED));
                served = true;
                continue;
            }

            var start = target.Kind == MessagePolling.Next ? NextOffset(partition) : target.Value;
            if (start >= Limit || CountLimited && held >= fetch.Count)
            {
                sections.Add(EmptySection(partition, start, ReadContext));
                continue;
            }

            served = true;
            held += GOLDEN_BATCH_MESSAGES;
            var section = GoldenSection(partition, start, ReadContext);
            section = ReportsHead ? section with { CurrentOffset = Limit - 1 } : section;
            sections.Add(Undecodable.Contains(start) ? section with { Records = [] } : section);
            if (fetch.AutoCommit)
            {
                AutoCommit(partition, start + GOLDEN_BATCH_MESSAGES - 1);
            }
        }

        if (served && fetch.AutoCommit && LoseNextCommittingRead)
        {
            LoseNextCommittingRead = false;
            OnLostCommittingRead?.Invoke();
            throw new IOException("Reply lost after storing.");
        }

        if (served && fetch.AutoCommit && AssignmentAfterRead is { } assignment)
        {
            Assignment = assignment;
            AssignmentAfterRead = null;
        }

        if (!served && sections.All(section => section.Status == 0))
        {
            Park(node, request, fetch.WaitUs, Reply(request.Operation, FetchBody([.. sections])));
            return null;
        }

        return Reply(request.Operation, FetchBody(Sectionless ? [] : [.. sections]));
    }

    private byte[]? Poll(int node, MockRequest request)
    {
        var (partition, kind, value, count) = ReadPoll(request);
        var autoCommit = request.Body[^(TcpContracts.DEFERRED_POLL_TAIL_SIZE + 1)] != 0;
        var received = new ReceivedPoll(partition, kind, value, autoCommit, count);
        Polls.Enqueue(received);
        if (ProbeRefusal is { } refusal && IsProbe(received))
        {
            return Reply(request.Operation, [], (uint)refusal);
        }

        // A read of owner generation 0 reads for the installed owner, so only a metadata op that the cluster did not
        // apply yet and another history or owner refuse it.
        if (request.Context.MetadataOp > AppliedOp)
        {
            return Reply(request.Operation, [], VsrError.TRANSIENT_NOT_ACCEPTED);
        }

        if (RefusesHistory(request))
        {
            return Reply(request.Operation, [], VsrError.HISTORY_UNAVAILABLE);
        }

        if (request.Context.OwnerGeneration != 0 && request.Context.OwnerGeneration != OwnerGeneration)
        {
            return Reply(request.Operation, [], VsrError.CONSUMER_GROUP_PARTITION_NOT_OWNED);
        }

        if (Primary(partition) != node)
        {
            return Reply(request.Operation, [], VsrError.NOT_PRIMARY);
        }

        if (kind == MessagePolling.Last && OversizedLast)
        {
            return Reply(request.Operation, [], VsrError.INVALID_SIZE_BYTES);
        }

        if (RecoveryNotOwned && kind == MessagePolling.Offset && !autoCommit)
        {
            return Reply(request.Operation, [], VsrError.CONSUMER_GROUP_PARTITION_NOT_OWNED);
        }

        if (RecoveryUnanswered && kind == MessagePolling.Offset && !autoCommit)
        {
            return null;
        }

        var start = kind switch
        {
            MessagePolling.Next => NextOffset(partition),
            MessagePolling.Offset => value,
            MessagePolling.Timestamp => value,
            MessagePolling.Last => Limit - GOLDEN_BATCH_MESSAGES,
            _ => 0ul
        };
        if (start >= Limit)
        {
            var empty = GoldenPollBody(partition, start, ReadContext)[..DeferredPollOptions.POLL_REPLY_HEADER_SIZE];
            BinaryPrimitives.WriteUInt32LittleEndian(empty.AsSpan(sizeof(uint) + sizeof(ulong)), 0);
            var tail = request.Body.AsSpan(request.Body.Length - TcpContracts.DEFERRED_POLL_TAIL_SIZE);
            Park(node, request, BinaryPrimitives.ReadUInt64LittleEndian(tail), Reply(request.Operation, empty));
            return null;
        }

        var body = GoldenPollBody(partition, start, ReadContext);
        if (Undecodable.Contains(start))
        {
            body = body[..^1];
        }

        if (autoCommit)
        {
            AutoCommit(partition, start + GOLDEN_BATCH_MESSAGES - 1);
            if (LoseNextCommittingRead)
            {
                LoseNextCommittingRead = false;
                OnLostCommittingRead?.Invoke();
                throw new IOException("Reply lost after storing.");
            }
        }

        return Reply(request.Operation, body);
    }

    /// <summary>Holds a read that finds nothing until its wait ends, then answers it with <paramref name="empty" />.</summary>
    private void Park(int node, MockRequest request, ulong waitUs, byte[] empty)
    {
        var key = (node, request.ClientId, request.RequestId);
        _parked[key] = request;
        _ = Task.Run(async () =>
        {
            await Task.Delay(TimeSpan.FromMicroseconds(waitUs));
            if (_parked.TryRemove(key, out _))
            {
                await _nodes[node].AnswerParkedAsync(request, empty);
            }
        });
    }

    private void AutoCommit(uint partition, ulong offset)
    {
        Stored.AddOrUpdate(partition, offset, (_, stored) => Math.Max(stored, offset));
    }

    private byte[] Store(MockRequest request)
    {
        StoreOwners.Enqueue(request.Context.OwnerGeneration);
        StoreOps.Enqueue(request.Context.MetadataOp);
        if (RefusesHistory(request))
        {
            return Reply(request.Operation, [], VsrError.HISTORY_UNAVAILABLE);
        }

        if (NextStoreNotOwned || StoresNotOwned)
        {
            NextStoreNotOwned = false;
            return Reply(request.Operation, [], VsrError.CONSUMER_GROUP_PARTITION_NOT_OWNED);
        }

        if (StoresRefused)
        {
            return Reply(request.Operation, [], VsrError.HISTORY_UNAVAILABLE);
        }

        if (RefusesOwner(request))
        {
            return Reply(request.Operation, [], VsrError.CONSUMER_GROUP_PARTITION_NOT_OWNED);
        }

        var stored = BinaryPrimitives.ReadUInt64LittleEndian(
            request.Body.AsSpan(^(StoredPartitionFromEnd - sizeof(uint))));
        if (stored >= Limit)
        {
            return Reply(request.Operation, [], VsrError.INVALID_OFFSET);
        }

        Stored[BinaryPrimitives.ReadUInt32LittleEndian(request.Body.AsSpan(^StoredPartitionFromEnd))] = stored;
        if (LoseNextStoreReply)
        {
            LoseNextStoreReply = false;
            return Reply(request.Operation, [], VsrError.REQUEST_TOO_OLD);
        }

        return Answer(request);
    }

    /// <summary>
    ///     The server refuses an offset write or lookup, and a read, of another partition history before any owner rule.
    ///     The current incarnation is never 0, so a request of incarnation 0 is always refused.
    /// </summary>
    private static bool RefusesHistory(MockRequest request)
    {
        return request.Context.Incarnation != Served.Incarnation;
    }

    /// <summary>
    ///     The server takes an offset write of owner generation 0 for the installed owner when the write routes by the
    ///     install op or a later one, and refuses a write that names another owner.
    /// </summary>
    private bool RefusesOwner(MockRequest request)
    {
        return request.Context.OwnerGeneration == 0
            ? request.Context.MetadataOp < InstallOp
            : request.Context.OwnerGeneration != OwnerGeneration;
    }

    private byte[] GroupReply(MockRequest request)
    {
        if (LoseNextGroupReply != (VsrOperation)request.Operation)
        {
            var reply = Answer(request);
            BinaryPrimitives.WriteUInt64LittleEndian(reply.AsSpan(VsrHeader.REPLY_COMMIT_OFFSET), InstallOp);
            return reply;
        }

        LoseNextGroupReply = null;
        return Reply(request.Operation, [], VsrError.REQUEST_TOO_OLD);
    }

    private static bool IsProbe(ReceivedPoll poll)
    {
        return poll is { Kind: MessagePolling.Next, AutoCommit: false, Count: 1 };
    }

    private static uint LookedUp(MockRequest request)
    {
        return BinaryPrimitives.ReadUInt32LittleEndian(request.Body.AsSpan(^LookedUpPartitionFromEnd));
    }

    private byte[] Offset(MockRequest request)
    {
        var partition = LookedUp(request);
        Lookups.Enqueue(partition);
        if (LookupRefusal is { } refusal)
        {
            return Reply(request.Operation, [], (uint)refusal);
        }

        if (RefusesHistory(request))
        {
            return Reply(request.Operation, [], VsrError.HISTORY_UNAVAILABLE);
        }

        if (!Stored.TryGetValue(partition, out var stored))
        {
            return Reply(request.Operation, []);
        }

        var body = new byte[OffsetReplySize];
        BinaryPrimitives.WriteUInt32LittleEndian(body, partition);
        BinaryPrimitives.WriteUInt64LittleEndian(body.AsSpan(sizeof(uint)), Math.Max(Limit, 1) - 1);
        BinaryPrimitives.WriteUInt64LittleEndian(body.AsSpan(sizeof(uint) + sizeof(ulong)), stored);
        return Reply(request.Operation, body);
    }

    private PartitionContext Context => Served with { OwnerGeneration = OwnerGeneration };

    private PartitionContext ReadContext => ReadsUnstamped ? default : Context;

    /// <summary>The metadata op that the cluster applied: the op of its routes, or a later install.</summary>
    private ulong AppliedOp => Math.Max(Served.MetadataOp, InstallOp);

    private ulong NextOffset(uint partition)
    {
        return Stored.TryGetValue(partition, out var stored) ? stored + 1 : 0;
    }

    private byte[] Roster()
    {
        var roster = new List<byte>();
        WriteString(roster, "reader-cluster");
        roster.AddRange(BitConverter.GetBytes((uint)_nodes.Length));
        for (var node = 0; node < _nodes.Length; node++)
        {
            WriteNode(roster, _nodes[node].Port, node == 0, (byte)node);
        }

        return Reply(OPERATION_NON_REPLICATED, roster.ToArray());
    }
}
