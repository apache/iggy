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

    internal ulong Limit { get; init; } = 2;

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
                return RouteReply(request, Served, primary, PartitionsCount, primary);
            case CommandCodes.SYNC_CONSUMER_GROUP_CODE:
                Interlocked.Increment(ref _syncs);
                return Reply(OPERATION_NON_REPLICATED, AssignmentBody(1, Assignment));
            case CommandCodes.FETCH_MESSAGES_CODE:
                return Fetch(node, request);
            case CommandCodes.POLL_MESSAGES_DEFERRED_CODE:
                return Poll(node, request);
            case CommandCodes.CANCEL_DEFERRED_REQUEST_CODE:
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

            if (HistoryRefused.Contains(partition))
            {
                sections.Add(EmptySection(partition, 0, default, VsrError.HISTORY_UNAVAILABLE));
                served = true;
                continue;
            }

            var start = target.Kind == MessagePolling.Next ? NextOffset(partition) : target.Value;
            if (start >= Limit || CountLimited && held >= fetch.Count)
            {
                sections.Add(EmptySection(partition, start, Served));
                continue;
            }

            served = true;
            held += GOLDEN_BATCH_MESSAGES;
            sections.Add(Undecodable.Contains(start)
                ? GoldenSection(partition, start, Served) with { Records = [] }
                : GoldenSection(partition, start, Served));
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

        var start = kind switch
        {
            MessagePolling.Next => NextOffset(partition),
            MessagePolling.Offset => value,
            MessagePolling.Last => Limit - GOLDEN_BATCH_MESSAGES,
            _ => 0ul
        };
        if (start >= Limit)
        {
            var empty = GoldenPollBody(partition, start, Served)[..DeferredPollOptions.POLL_REPLY_HEADER_SIZE];
            BinaryPrimitives.WriteUInt32LittleEndian(empty.AsSpan(sizeof(uint) + sizeof(ulong)), 0);
            var tail = request.Body.AsSpan(request.Body.Length - TcpContracts.DEFERRED_POLL_TAIL_SIZE);
            Park(node, request, BinaryPrimitives.ReadUInt64LittleEndian(tail), Reply(request.Operation, empty));
            return null;
        }

        var body = GoldenPollBody(partition, start, Served);
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
