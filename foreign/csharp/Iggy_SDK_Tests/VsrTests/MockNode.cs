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
using System.Net;
using System.Net.Sockets;
using System.Text;
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
    internal const int PING_CODE = 1;
    internal const uint TRANSIENT_NOT_ACCEPTED = 58;

    /// <summary>A reply for anything the roster read does not claim: a register, or an empty read.</summary>
    internal static byte[] Answer(MockRequest request)
    {
        return request.Operation == OPERATION_REGISTER
            ? Reply(OPERATION_REGISTER, RegisterBody(session: 128))
            : Reply(OPERATION_NON_REPLICATED, []);
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
        BinaryPrimitives.WriteUInt32LittleEndian(frame.AsSpan(REPLY_STATUS_OFFSET, 4), status);
        body.CopyTo(frame.AsSpan(HEADER_SIZE));

        return frame;
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
        BinaryPrimitives.WriteUInt32LittleEndian(payload[12..16], 11 << 10);
        payload[16] = (byte)serverVersion.Length;
        serverVersion.CopyTo(payload[17..]);

        return body;
    }
}

internal readonly record struct MockRequest(byte Operation, int Code, ulong RequestId);

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

    public void Serve(Func<MockRequest, byte[]> handler)
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

                _ = Task.Run(() => Exchange(connection, handler));
            }
        });
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

    private async Task Exchange(TcpClient connection, Func<MockRequest, byte[]> handler)
    {
        try
        {
            await using var stream = connection.GetStream();
            var header = new byte[HEADER_SIZE];
            while (!_killed)
            {
                await ReadExactly(stream, header);
                var size = BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(SIZE_OFFSET, 4));
                var body = new byte[size - HEADER_SIZE];
                await ReadExactly(stream, body);

                var request = new MockRequest(header[REQUEST_OPERATION_OFFSET],
                    BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(REQUEST_RESERVED_OFFSET, 4)),
                    BinaryPrimitives.ReadUInt64LittleEndian(header.AsSpan(REQUEST_ID_OFFSET, 8)));
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

                var reply = handler(request);
                BinaryPrimitives.WriteUInt64LittleEndian(reply.AsSpan(REPLY_REQUEST_ID_OFFSET, 8),
                    request.RequestId);
                await stream.WriteAsync(reply);
                await stream.FlushAsync();
            }
        }
        catch (Exception)
        {
            // A killed node and a client that went away look the same here.
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
