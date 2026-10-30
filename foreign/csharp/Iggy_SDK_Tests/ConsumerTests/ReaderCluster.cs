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
internal readonly record struct ReceivedPoll(uint PartitionId, MessagePolling Kind, ulong Value, bool AutoCommit);

/// <summary>
///     Nodes that each lead the partitions that <see cref="Primary" /> names. A partition holds the offsets 0 to
///     <see cref="Limit" /> minus 1, and a read serves two messages from its start. A read of a partition that
///     this node does not lead is refused as not primary.
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

    private readonly ConcurrentDictionary<uint, int> _moved = new();
    private readonly MockNode[] _nodes;
    private readonly ConcurrentDictionary<(int Node, UInt128 Client, ulong Request), MockRequest> _parked = new();
    private readonly ConcurrentQueue<(int Node, MockRequest Request)> _held = new();
    private int _cancels;
    private int _joins;
    private int _leaves;
    private int _syncs;
    private int _routeLookups;

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

    /// <summary>The owner generation of every partition. A read that names another one is refused as not owned.</summary>
    internal ulong OwnerGeneration { get; set; } = Served.OwnerGeneration;

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

    internal int HeldFetches => _held.Count;

    /// <summary>The fetches that wait for <see cref="ReleaseHeldAsync" />, as the nodes received them.</summary>
    internal IReadOnlyList<FetchRequest> Held => [.. _held.Select(held => ReadFetch(held.Request))];

    /// <summary>A served section reports the head of its partition, as the server does, instead of its last offset.</summary>
    internal bool ReportsHead { get; init; }

    internal bool OversizedLast { get; init; }

    internal bool RecoveryNotOwned { get; set; }

    internal bool LoseNextCommittingRead { get; set; }

    internal Action? OnLostCommittingRead { get; set; }

    internal uint[] Assignment { get; set; } = [];

    /// <summary>The assignment that the member moves to once a committing fetch is served.</summary>
    internal uint[]? AssignmentAfterRead { get; set; }

    internal HashSet<ulong> Undecodable { get; } = [];

    internal ConcurrentBag<uint> HistoryRefused { get; } = [];

    internal ConcurrentDictionary<uint, ulong> Stored { get; } = new();

    internal ConcurrentQueue<ReceivedFetch> Fetches { get; } = new();

    internal ConcurrentQueue<ReceivedPoll> Polls { get; } = new();

    internal int Cancels => Volatile.Read(ref _cancels);

    internal int Joins => Volatile.Read(ref _joins);

    internal int Leaves => Volatile.Read(ref _leaves);

    internal int Syncs => Volatile.Read(ref _syncs);

    /// <summary>Refuses every route lookup, so the reader cannot plan a read.</summary>
    internal bool RoutesRefused { get; set; }

    internal int RouteLookups => Volatile.Read(ref _routeLookups);

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
    internal async Task ReleaseHeldAsync(int count = int.MaxValue)
    {
        while (count-- > 0 && _held.TryDequeue(out var held))
        {
            if (Fetch(held.Node, held.Request) is { } reply)
            {
                await _nodes[held.Node].AnswerParkedAsync(held.Request, reply);
            }
        }
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

    private byte[]? Handle(int node, MockRequest request)
    {
        switch ((VsrOperation)request.Operation)
        {
            case VsrOperation.StoreConsumerOffset when StoresRefused:
                return Reply(request.Operation, [], VsrError.HISTORY_UNAVAILABLE);
            case VsrOperation.StoreConsumerOffset:
                Stored[BinaryPrimitives.ReadUInt32LittleEndian(request.Body.AsSpan(^StoredPartitionFromEnd))] =
                    BinaryPrimitives.ReadUInt64LittleEndian(
                        request.Body.AsSpan(^(StoredPartitionFromEnd - sizeof(uint))));
                return Answer(request);
            case VsrOperation.DeleteConsumerOffset:
                Stored.TryRemove(BinaryPrimitives.ReadUInt32LittleEndian(
                    request.Body.AsSpan(^DeletedPartitionFromEnd)), out _);
                return Answer(request);
            case VsrOperation.JoinConsumerGroup:
                Interlocked.Increment(ref _joins);
                return Answer(request);
            case VsrOperation.LeaveConsumerGroup:
                Interlocked.Increment(ref _leaves);
                return Answer(request);
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
                var primary = (byte)Primary(routed);
                return RouteReply(request, Context, primary, PartitionsCount, primary);
            case CommandCodes.SYNC_CONSUMER_GROUP_CODE:
                Interlocked.Increment(ref _syncs);
                return Reply(OPERATION_NON_REPLICATED, AssignmentBody(1, Assignment));
            case CommandCodes.FETCH_MESSAGES_CODE when HoldFetches:
                _held.Enqueue((node, request));
                return null;
            case CommandCodes.FETCH_MESSAGES_CODE:
                return Fetch(node, request);
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

            var start = target.Kind == MessagePolling.Next ? NextOffset(partition) : target.Value;
            if (start >= Limit || CountLimited && held >= fetch.Count)
            {
                sections.Add(EmptySection(partition, start, Context));
                continue;
            }

            served = true;
            held += GOLDEN_BATCH_MESSAGES;
            var section = GoldenSection(partition, start, Context);
            section = ReportsHead ? section with { CurrentOffset = Limit - 1 } : section;
            sections.Add(Undecodable.Contains(start) ? section with { Records = [] } : section);
            if (fetch.AutoCommit)
            {
                Stored[partition] = start + GOLDEN_BATCH_MESSAGES - 1;
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
        var (partition, kind, value, _) = ReadPoll(request);
        var autoCommit = request.Body[^(TcpContracts.DEFERRED_POLL_TAIL_SIZE + 1)] != 0;
        Polls.Enqueue(new ReceivedPoll(partition, kind, value, autoCommit));
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
            MessagePolling.Last => Limit - GOLDEN_BATCH_MESSAGES,
            _ => 0ul
        };
        if (start >= Limit)
        {
            var empty = GoldenPollBody(partition, start, Context)[..DeferredPollOptions.POLL_REPLY_HEADER_SIZE];
            BinaryPrimitives.WriteUInt32LittleEndian(empty.AsSpan(sizeof(uint) + sizeof(ulong)), 0);
            var tail = request.Body.AsSpan(request.Body.Length - TcpContracts.DEFERRED_POLL_TAIL_SIZE);
            Park(node, request, BinaryPrimitives.ReadUInt64LittleEndian(tail), Reply(request.Operation, empty));
            return null;
        }

        var body = GoldenPollBody(partition, start, Context);
        if (Undecodable.Contains(start))
        {
            body = body[..^1];
        }

        if (autoCommit)
        {
            Stored[partition] = start + GOLDEN_BATCH_MESSAGES - 1;
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

    private PartitionContext Context => Served with { OwnerGeneration = OwnerGeneration };

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
