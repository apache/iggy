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
using System.Net;
using System.Net.Sockets;
using System.Text;
using Apache.Iggy.Contracts;
using Apache.Iggy.Contracts.Tcp;
using Apache.Iggy.Enums;
using Apache.Iggy.Mappers;
using Apache.Iggy.Tests.ContractsTests;
using Apache.Iggy.Utils;
using Apache.Iggy.Vsr;
using static Apache.Iggy.Tests.VsrTests.MockFrames;

namespace Apache.Iggy.Tests.VsrTests;

/// <summary>VSR frame layout and reply builders shared by the loopback mock node tests.</summary>
internal static class MockFrames
{
    internal const int HEADER_SIZE = 256;
    internal const int SIZE_OFFSET = 48;
    internal const int COMMAND_OFFSET = 60;
    internal const int REQUEST_ID_OFFSET = 168;
    internal const int REQUEST_OPERATION_OFFSET = 176;
    internal const int REQUEST_RESERVED_OFFSET = 196;
    internal const int REQUEST_CONTEXT_OFFSET = 200;
    internal const int REPLY_REQUEST_ID_OFFSET = 200;
    internal const int REPLY_OPERATION_OFFSET = 208;
    internal const int REPLY_STATUS_OFFSET = 216;

    internal const byte COMMAND_REPLY = 8;
    internal const byte COMMAND_EVICTION = 13;
    internal const int EVICTION_REASON_OFFSET = 255;
    internal const byte EVICTION_STALE_CLIENT = 13;
    internal const byte OPERATION_REGISTER = 1;
    internal const byte OPERATION_NON_REPLICATED = 2;
    internal const int GET_CLUSTER_METADATA_CODE = 12;
    internal const int BIND_SESSION_CODE = 15;
    internal const int PING_CODE = 1;
    internal const uint TRANSIENT_NOT_ACCEPTED = 58;

    /// <summary>A target prefix: the partition id, then the strategy kind and value.</summary>
    private const int PartitionStrategySize = sizeof(uint) + 1 + sizeof(ulong);

    /// <summary>What a PollMessages body carries after its partition id: the strategy, the count, the commit flag.</summary>
    private const int PollTailAfterPartition = 1 + sizeof(ulong) + sizeof(uint) + 1;

    /// <summary>A FetchMessages target: the partition id, the strategy, the byte limit and the fence.</summary>
    private const int FetchTargetSize = PartitionStrategySize + sizeof(uint) + 2 * sizeof(ulong);

    /// <summary>The wire numbers the polling kinds from 1, in the order of <see cref="MessagePolling" />.</summary>
    private const int FirstWireKind = 1;

    /// <summary>A poll reply body starts with the partition, the current offset, the count and the context.</summary>
    private const int PollContextPosition = sizeof(uint) + sizeof(ulong) + sizeof(uint);

    private const int PollHeaderSize = PollContextPosition + PartitionContext.ENCODED_SIZE;

    /// <summary>The base offset of the golden batch, counted from the start of its records.</summary>
    private const int RecordsBaseOffsetPosition = sizeof(ulong);

    /// <summary>How many messages the golden batch holds.</summary>
    internal const uint GOLDEN_BATCH_MESSAGES = 2;

    /// <summary>A reply for anything the roster read does not claim: a register, or an empty read.</summary>
    internal static byte[] Answer(MockRequest request)
    {
        if (request.Code == CommandCodes.GET_PARTITION_ROUTE_CODE)
        {
            return RouteReply(request, new PartitionContext(17, 0, 1), 0);
        }
        if (request.Code == BIND_SESSION_CODE)
        {
            return Reply(OPERATION_NON_REPLICATED, RegisterBody(request.Session)[4..]);
        }
        return request.Operation == OPERATION_REGISTER
            ? Reply(OPERATION_REGISTER, RegisterBody(session: 128))
            : Reply(request.Operation, ((VsrOperation)request.Operation).IsResultFramed() ? new byte[4] : []);
    }

    internal static byte[] Reply(byte operation, byte[] body)
    {
        return Reply(operation, body, 0);
    }

    internal static byte[] Reply(byte operation, byte[] body, uint status)
    {
        var frame = new byte[HEADER_SIZE + body.Length];
        BinaryPrimitives.WriteUInt32LittleEndian(frame.AsSpan(SIZE_OFFSET, 4), (uint)frame.Length);
        frame[COMMAND_OFFSET] = COMMAND_REPLY;
        frame[REPLY_OPERATION_OFFSET] = operation;
        if (status == 0 && operation is not OPERATION_NON_REPLICATED)
        {
            frame[VsrHeader.REPLY_COMMITTED_OFFSET] = VsrHeader.COMMITTED_RESULT_MARKER;
        }
        BinaryPrimitives.WriteUInt32LittleEndian(frame.AsSpan(REPLY_STATUS_OFFSET, 4), status);
        body.CopyTo(frame.AsSpan(HEADER_SIZE));

        return frame;
    }

    internal static void CorrelateReply(MockRequest request, byte[] reply)
    {
        BinaryPrimitives.WriteUInt64LittleEndian(reply.AsSpan(REPLY_REQUEST_ID_OFFSET), request.RequestId);
        BinaryPrimitives.WriteUInt128LittleEndian(reply.AsSpan(VsrHeader.REPLY_CLIENT_OFFSET), request.ClientId);
        BinaryPrimitives.WriteUInt128LittleEndian(reply.AsSpan(VsrHeader.REPLY_CHECKSUM_OFFSET), request.RequestChecksum);
        BinaryPrimitives.WriteUInt64LittleEndian(reply.AsSpan(VsrHeader.REPLY_INCARNATION_OFFSET), request.Context.Incarnation);
        if (VsrHeader.ReadCommitted(reply))
        {
            var commit = BinaryPrimitives.ReadUInt64LittleEndian(reply.AsSpan(VsrHeader.REPLY_COMMIT_OFFSET));
            commit = Math.Max(1, commit);
            BinaryPrimitives.WriteUInt64LittleEndian(reply.AsSpan(VsrHeader.REPLY_COMMIT_OFFSET), commit);
            BinaryPrimitives.WriteUInt64LittleEndian(reply.AsSpan(VsrHeader.REPLY_OP_OFFSET), commit);
        }
    }

    /// <summary>The roster a standalone server reports: itself, as the leader.</summary>
    internal static byte[] StandaloneRoster(ushort port)
    {
        var body = new List<byte>();
        WriteString(body, "standalone");
        body.AddRange(BitConverter.GetBytes(1u));
        WriteNode(body, port, true);
        return Reply(OPERATION_NON_REPLICATED, body.ToArray());
    }

    internal static byte[] RouteReply(MockRequest request, PartitionContext context, byte primaryReplica,
        uint partitionsCount = 1, uint view = 0)
    {
        var body = new byte[PartitionRoute.ENCODED_SIZE];
        BinaryPrimitives.WriteUInt64LittleEndian(body, context.Incarnation);
        BinaryPrimitives.WriteUInt64LittleEndian(body.AsSpan(8), context.MetadataOp);
        BinaryPrimitives.WriteUInt32LittleEndian(body.AsSpan(16), view);
        BinaryPrimitives.WriteUInt32LittleEndian(body.AsSpan(20), partitionsCount);
        body[24] = primaryReplica;
        return Reply(OPERATION_NON_REPLICATED, body);
    }

    /// <summary>A SyncConsumerGroup reply body: the assignment generation and the member's partitions.</summary>
    internal static byte[] AssignmentBody(ulong generation, uint[] partitions)
    {
        var body = new byte[12 + partitions.Length * 4];
        BinaryPrimitives.WriteUInt64LittleEndian(body.AsSpan(0, 8), generation);
        BinaryPrimitives.WriteUInt32LittleEndian(body.AsSpan(8, 4), (uint)partitions.Length);
        for (var index = 0; index < partitions.Length; index++)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(body.AsSpan(12 + index * 4, 4), partitions[index]);
        }

        return body;
    }

    /// <summary>A FetchMessages request as the node read it.</summary>
    internal static FetchRequest ReadFetch(MockRequest request)
    {
        var body = request.Body.AsSpan();
        var position = 1;
        for (var identifier = 0; identifier < 3; identifier++)
        {
            position += 2 + body[position + 1];
        }

        var waitUs = BinaryPrimitives.ReadUInt64LittleEndian(body[position..]);
        var requestTimeoutUs = BinaryPrimitives.ReadUInt64LittleEndian(body[(position + sizeof(ulong))..]);
        position += 2 * sizeof(ulong);
        var minCount = BinaryPrimitives.ReadUInt32LittleEndian(body[position..]);
        var count = BinaryPrimitives.ReadUInt32LittleEndian(body[(position + sizeof(uint))..]);
        var maxBytes = BinaryPrimitives.ReadUInt32LittleEndian(body[(position + 2 * sizeof(uint))..]);
        position += 3 * sizeof(uint);
        var autoCommit = body[position++] == 1;
        var targets = new FetchTarget[BinaryPrimitives.ReadUInt32LittleEndian(body[position..])];
        position += sizeof(uint);
        for (var index = 0; index < targets.Length; index++)
        {
            var target = body[(position + index * FetchTargetSize)..];
            targets[index] = new FetchTarget(BinaryPrimitives.ReadUInt32LittleEndian(target),
                (MessagePolling)(target[sizeof(uint)] - FirstWireKind),
                BinaryPrimitives.ReadUInt64LittleEndian(target[(sizeof(uint) + 1)..]),
                BinaryPrimitives.ReadUInt32LittleEndian(target[PartitionStrategySize..]),
                BinaryPrimitives.ReadUInt64LittleEndian(target[(PartitionStrategySize + sizeof(uint))..]),
                BinaryPrimitives.ReadUInt64LittleEndian(target[(PartitionStrategySize + sizeof(uint) + sizeof(ulong))..]));
        }

        return new FetchRequest(waitUs, requestTimeoutUs, minCount, count, maxBytes, autoCommit, targets);
    }

    /// <summary>A FetchMessages reply body: the section count, the section table, then the records.</summary>
    internal static byte[] FetchBody(params FetchSection[] sections)
    {
        var body = new byte[FetchedMessages.HeadSize(sections.Length) + sections.Sum(section => section.Records.Length)];
        BinaryPrimitives.WriteUInt32LittleEndian(body, (uint)sections.Length);
        var records = FetchedMessages.HeadSize(sections.Length);
        for (var index = 0; index < sections.Length; index++)
        {
            var section = sections[index];
            var header = body.AsSpan(FetchedMessages.HeadSize(index));
            BinaryPrimitives.WriteUInt32LittleEndian(header, section.PartitionId);
            BinaryPrimitives.WriteUInt32LittleEndian(header[4..], section.Status);
            BinaryPrimitives.WriteUInt64LittleEndian(header[8..], section.CurrentOffset);
            BinaryPrimitives.WriteUInt32LittleEndian(header[16..], section.MessagesCount);
            BinaryPrimitives.WriteUInt64LittleEndian(header[20..], section.Context.Incarnation);
            BinaryPrimitives.WriteUInt64LittleEndian(header[28..], section.Context.OwnerGeneration);
            BinaryPrimitives.WriteUInt64LittleEndian(header[36..], section.Context.MetadataOp);
            BinaryPrimitives.WriteUInt64LittleEndian(header[44..], section.NextOffset);
            BinaryPrimitives.WriteUInt32LittleEndian(header[52..], (uint)section.Records.Length);
            section.Records.CopyTo(body.AsSpan(records));
            records += section.Records.Length;
        }

        return body;
    }

    /// <summary>
    ///     A section that serves the golden batch from <paramref name="firstOffset" />, so it holds that offset and
    ///     the next one.
    /// </summary>
    internal static FetchSection GoldenSection(uint partitionId, ulong firstOffset, PartitionContext context)
    {
        var records = GoldenPollBody(partitionId, firstOffset, context)[PollHeaderSize..];
        return new FetchSection(partitionId, 0, firstOffset + GOLDEN_BATCH_MESSAGES - 1, GOLDEN_BATCH_MESSAGES,
            context, firstOffset + GOLDEN_BATCH_MESSAGES, records);
    }

    /// <summary>The golden poll body moved to another partition, first offset and context.</summary>
    internal static byte[] GoldenPollBody(uint partitionId, ulong firstOffset, PartitionContext context)
    {
        var body = Convert.FromHexString(MessageBatchGoldenVectorTests.POLL_BODY);
        BinaryPrimitives.WriteUInt32LittleEndian(body, partitionId);
        BinaryPrimitives.WriteUInt64LittleEndian(body.AsSpan(PollContextPosition), context.Incarnation);
        BinaryPrimitives.WriteUInt64LittleEndian(body.AsSpan(PollContextPosition + sizeof(ulong)),
            context.OwnerGeneration);
        BinaryPrimitives.WriteUInt64LittleEndian(body.AsSpan(PollContextPosition + 2 * sizeof(ulong)),
            context.MetadataOp);
        BinaryPrimitives.WriteUInt64LittleEndian(body.AsSpan(PollHeaderSize + RecordsBaseOffsetPosition), firstOffset);

        return body;
    }

    /// <summary>A section that read nothing: an empty read without a status, or a refusal with one.</summary>
    internal static FetchSection EmptySection(uint partitionId, ulong nextOffset, PartitionContext context,
        uint status = 0)
    {
        return status == 0
            ? new FetchSection(partitionId, 0, nextOffset, 0, context, nextOffset, [])
            : new FetchSection(partitionId, status, 0, 0, default, 0, []);
    }

    /// <summary>The PollMessages part of a poll request: a deferred poll appends its wait options to it.</summary>
    internal static ReadOnlySpan<byte> PollBody(MockRequest request)
    {
        var tail = request.Code == CommandCodes.POLL_MESSAGES_DEFERRED_CODE ? TcpContracts.DEFERRED_POLL_TAIL_SIZE : 0;
        return request.Body.AsSpan(0, request.Body.Length - tail);
    }

    /// <summary>The partition a PollMessages or PollMessagesDeferred request reads.</summary>
    internal static uint PolledPartition(MockRequest request)
    {
        return BinaryPrimitives.ReadUInt32LittleEndian(PollBody(request)[^(PollTailAfterPartition + sizeof(uint))..]);
    }

    /// <summary>The partition, the strategy and the count of a PollMessages or PollMessagesDeferred request.</summary>
    internal static (uint PartitionId, MessagePolling Kind, ulong Value, uint Count) ReadPoll(MockRequest request)
    {
        var tail = PollBody(request)[^PollTailAfterPartition..];
        return (PolledPartition(request), (MessagePolling)(tail[0] - FirstWireKind),
            BinaryPrimitives.ReadUInt64LittleEndian(tail[1..]),
            BinaryPrimitives.ReadUInt32LittleEndian(tail[(1 + sizeof(ulong))..]));
    }

    /// <summary>A FetchMessages target as the client sent it, with its fence.</summary>
    internal readonly record struct FetchTarget(
        uint PartitionId,
        MessagePolling Kind,
        ulong Value,
        uint MaxBytes,
        ulong Incarnation,
        ulong OwnerGeneration);

    internal readonly record struct FetchRequest(
        ulong WaitUs,
        ulong RequestTimeoutUs,
        uint MinCount,
        uint Count,
        uint MaxBytes,
        bool AutoCommit,
        FetchTarget[] Targets);

    /// <summary>One section of a FetchMessages reply. The records are those a poll reply carries after its header.</summary>
    internal readonly record struct FetchSection(
        uint PartitionId,
        uint Status,
        ulong CurrentOffset,
        uint MessagesCount,
        PartitionContext Context,
        ulong NextOffset,
        byte[] Records);

    internal static void WriteNode(List<byte> body, ushort port, bool leader, byte replicaId = 0)
    {
        body.Add(replicaId);
        WriteString(body, $"node-{port}");
        WriteString(body, "127.0.0.1");
        body.AddRange(BitConverter.GetBytes(port));
        body.AddRange(new byte[6]);
        body.Add(leader ? (byte)0 : (byte)1);
        body.Add(0);
    }

    internal static void WriteString(List<byte> body, string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        body.AddRange(BitConverter.GetBytes((uint)bytes.Length));
        body.AddRange(bytes);
    }

    /// <summary>
    ///     A register reply carries a committed result section, so its four leading zero bytes announce zero
    ///     entries and the typed payload starts right after them. A non-replicated read carries none.
    /// </summary>
    internal static byte[] RegisterBody(ulong session)
    {
        var serverVersion = Encoding.UTF8.GetBytes("0.0.0");
        var body = new byte[4 + 17 + serverVersion.Length];
        var payload = body.AsSpan(4);
        BinaryPrimitives.WriteUInt32LittleEndian(payload[..4], 7);
        BinaryPrimitives.WriteUInt64LittleEndian(payload[4..12], session);
        BinaryPrimitives.WriteUInt32LittleEndian(payload[12..16], Apache.Iggy.Vsr.LoginRegister.PROTOCOL_VERSION);
        payload[16] = (byte)serverVersion.Length;
        serverVersion.CopyTo(payload[17..]);

        return body;
    }
}

internal readonly record struct MockRequest(byte Operation, int Code, ulong RequestId)
{
    internal UInt128 ClientId { get; init; }
    internal UInt128 RequestChecksum { get; init; }
    internal ulong Session { get; init; }
    internal PartitionContext Context { get; init; }
    internal byte[] Body { get; init; } = [];
}

/// <summary>
///     A loopback VSR node. Killing it drops the live sockets and stops accepting, so a redial is refused the
///     way a dead process refuses one.
/// </summary>
internal sealed class MockNode : IDisposable
{
    private readonly TcpListener _listener;
    private readonly List<TcpClient> _accepted = [];
    private volatile bool _killed;
    private int _connections;
    private readonly List<MockRequest> _recorded = [];
    private readonly ConcurrentDictionary<(UInt128 Client, ulong Request), Func<byte[], Task>> _parked = new();
    private int _pings;
    private int _registrations;

    /// <param name="port">
    ///     A port to bind, for a node that has to come up on an address the client already knows. Zero
    ///     takes whatever the OS hands out.
    /// </param>
    public MockNode(ushort port = 0)
    {
        _listener = new TcpListener(IPAddress.Loopback, port);
        _listener.Start();
        Port = (ushort)((IPEndPoint)_listener.LocalEndpoint).Port;
    }

    public ushort Port { get; }

    public int Pings => Volatile.Read(ref _pings);

    public int Registrations => Volatile.Read(ref _registrations);

    /// <summary>How many requests with the given command code the node has answered so far.</summary>
    public int Requests(int code)
    {
        lock (_recorded)
        {
            return _recorded.Count(request => request.Code == code);
        }
    }

    public int ActiveConnections
    {
        get
        {
            lock (_accepted)
            {
                return _accepted.Count;
            }
        }
    }

    public int Connections
    {
        get
        {
            lock (_accepted)
            {
                return _connections;
            }
        }
    }

    /// <param name="handler">
    ///     Answers each request, or returns null to leave it unanswered, the way the server holds a parked deferred
    ///     request and never answers its cancel.
    /// </param>
    /// <param name="beforeSend">Runs before each reply goes out. The reply waits until its task ends.</param>
    public void Serve(Func<MockRequest, byte[]?> handler, Func<MockRequest, byte[], Task>? beforeSend = null)
    {
        _ = Task.Run(async () =>
        {
            while (!_killed)
            {
                TcpClient connection;
                try
                {
                    connection = await _listener.AcceptTcpClientAsync();
                }
                catch (Exception)
                {
                    return;
                }

                lock (_accepted)
                {
                    _accepted.Add(connection);
                    _connections++;
                }

                _ = Task.Run(() => Exchange(connection, handler, beforeSend));
            }
        });
    }

    /// <summary>Sends the reply of a request that the handler left unanswered, on the link that carried it.</summary>
    public Task AnswerParkedAsync(MockRequest request, byte[] reply)
    {
        if (!_parked.TryRemove((request.ClientId, request.RequestId), out var send))
        {
            throw new InvalidOperationException($"Request {request.RequestId} is not parked.");
        }

        CorrelateReply(request, reply);
        return send(reply);
    }

    public void Kill()
    {
        _killed = true;
        lock (_accepted)
        {
            foreach (var connection in _accepted)
            {
                connection.Close();
            }

            _accepted.Clear();
        }

        _listener.Stop();
    }

    public void Dispose()
    {
        Kill();
    }

    private async Task Exchange(TcpClient connection, Func<MockRequest, byte[]?> handler,
        Func<MockRequest, byte[], Task>? beforeSend)
    {
        try
        {
            await using var stream = connection.GetStream();
            using var writes = new SemaphoreSlim(1, 1);
            async Task SendAsync(byte[] frame)
            {
                await writes.WaitAsync();
                try
                {
                    await stream.WriteAsync(frame);
                    await stream.FlushAsync();
                }
                finally
                {
                    writes.Release();
                }
            }

            var header = new byte[HEADER_SIZE];
            while (!_killed)
            {
                await ReadExactly(stream, header);
                var size = BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(SIZE_OFFSET, 4));
                var body = new byte[size - HEADER_SIZE];
                await ReadExactly(stream, body);

                var request = new MockRequest(header[REQUEST_OPERATION_OFFSET],
                    BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(REQUEST_RESERVED_OFFSET, 4)),
                    BinaryPrimitives.ReadUInt64LittleEndian(header.AsSpan(REQUEST_ID_OFFSET, 8)))
                {
                    ClientId = BinaryPrimitives.ReadUInt128LittleEndian(header.AsSpan(VsrHeader.REQUEST_CLIENT_OFFSET)),
                    RequestChecksum = BinaryPrimitives.ReadUInt128LittleEndian(header.AsSpan(VsrHeader.REQUEST_CHECKSUM_OFFSET)),
                    Session = BinaryPrimitives.ReadUInt64LittleEndian(header.AsSpan(184)),
                    Context = BinaryMapper.MapPartitionContext(header.AsSpan(REQUEST_CONTEXT_OFFSET)),
                    Body = body
                };
                lock (_recorded)
                {
                    _recorded.Add(request);
                }

                if (request.Operation == OPERATION_REGISTER)
                {
                    Interlocked.Increment(ref _registrations);
                }
                else if (request.Code == PING_CODE)
                {
                    Interlocked.Increment(ref _pings);
                }

                if (handler(request) is not { } reply)
                {
                    _parked[(request.ClientId, request.RequestId)] = SendAsync;
                    continue;
                }
                CorrelateReply(request, reply);
                if (beforeSend is not null)
                {
                    await beforeSend(request, reply);
                }
                await SendAsync(reply);
            }
        }
        catch (Exception)
        {
            // A killed node and a client that went away look the same here.
        }
        finally
        {
            connection.Dispose();
            lock (_accepted)
            {
                _accepted.Remove(connection);
            }
        }
    }

    private static async Task ReadExactly(NetworkStream stream, byte[] buffer)
    {
        var read = 0;
        while (read < buffer.Length)
        {
            var chunk = await stream.ReadAsync(buffer.AsMemory(read));
            if (chunk == 0)
            {
                throw new EndOfStreamException("Connection closed");
            }

            read += chunk;
        }
    }
}
