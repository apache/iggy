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
using Apache.Iggy.Configuration;
using Apache.Iggy.Contracts.Tcp;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.IggyClient;
using Apache.Iggy.IggyClient.Implementations;
using Apache.Iggy.Vsr;
using Microsoft.Extensions.Logging.Abstractions;

namespace Apache.Iggy.Tests.VsrTests;

/// <summary>
///     The node a client signed in on dies; its next request has to complete on a survivor the roster named,
///     under a session established there. Mirrors
///     <c>core/integration/tests/cluster/failover_client_continuity.rs</c>.
/// </summary>
public sealed class EndpointFailoverTests
{
    private const int HeaderSize = 256;
    private const int SizeOffset = 48;
    private const int CommandOffset = 60;
    private const int RequestIdOffset = 168;
    private const int RequestOperationOffset = 176;
    private const int RequestReservedOffset = 196;
    private const int ReplyRequestIdOffset = 200;
    private const int ReplyOperationOffset = 208;
    private const int ReplyStatusOffset = 216;

    private const byte CommandReply = 8;
    private const byte CommandEviction = 13;
    private const int EvictionReasonOffset = 255;
    private const byte EvictionStaleClient = 13;
    private const byte OperationRegister = 1;
    private const byte OperationNonReplicated = 2;
    private const int GetClusterMetadataCode = 12;
    private const int PingCode = 1;

    [Fact]
    public async Task ResumesOnASurvivorAfterTheSignedInNodeDies()
    {
        using var primary = new MockNode();
        using var survivor = new MockNode();

        // The primary leads, so the sign-in settles there and the roster is only remembered - not acted on -
        // until the node dies.
        primary.Serve(request => request.Code == GetClusterMetadataCode
            ? Reply(OperationNonReplicated, ClusterMetadata(primary.Port, survivor.Port, primary.Port))
            : Answer(request));
        survivor.Serve(request => request.Code == GetClusterMetadataCode
            ? Reply(OperationNonReplicated, ClusterMetadata(primary.Port, survivor.Port, survivor.Port))
            : Answer(request));

        var configuration = new IggyClientConfigurator
        {
            BaseAddress = $"127.0.0.1:{primary.Port}",
            Protocol = Protocol.Tcp,
            ReconnectionSettings = new ReconnectionSettings
            {
                Enabled = true,
                MaxRetries = 4,
                InitialDelay = TimeSpan.FromMilliseconds(20)
            }
        };
        using var client = new TcpMessageStream(configuration, NullLoggerFactory.Instance);

        await client.ConnectAsync(TestContext.Current.CancellationToken);
        // No auto login: the credentials come from the caller's own sign-in, which is the shape that could not
        // reconnect at all before.
        await client.LoginUserAsync("iggy", "iggy", TestContext.Current.CancellationToken);
        await client.PingAsync(TestContext.Current.CancellationToken);
        Assert.Equal(1, primary.Pings);

        primary.Kill();

        // The request in flight when the node died is allowed to fail; what is not allowed is never completing
        // one, which is what a client that only knows the dead endpoint does.
        var (resumed, lastError) = await ResumedWithin(client, TimeSpan.FromSeconds(10));
        Assert.True(resumed,
            $"the client has to resume on the survivor the roster named ({lastError}, survivor saw " +
            $"{survivor.Registrations} registrations and {survivor.Pings} pings)");
        Assert.True(survivor.Registrations >= 1, "the remembered credentials signed in again on the survivor");
        Assert.True(survivor.Pings >= 1, "the request landed on the survivor");
    }

    /// <summary>
    ///     Mirrors the integration contract (HeartbeatTests
    ///     EvictedClient_WithoutAutoLogin_Should_FailFast_And_NotReconnect): a server-side eviction ends the
    ///     session authoritatively, so the credentials a manual sign-in remembered must not resurrect it - the
    ///     evicted request surfaces the loss with no reconnect attempt.
    /// </summary>
    [Fact]
    public async Task ServerEvictionForgetsTheRememberedSignIn()
    {
        using var node = new MockNode();
        var evict = false;
        node.Serve(request =>
        {
            if (request.Operation == OperationRegister)
            {
                return Reply(OperationRegister, RegisterBody(session: 128));
            }

            return evict
                ? EvictionFrame(EvictionStaleClient)
                : Reply(OperationNonReplicated, request.Code == GetClusterMetadataCode
                    ? ClusterMetadata(node.Port, node.Port, node.Port)
                    : []);
        });

        var configuration = new IggyClientConfigurator
        {
            BaseAddress = $"127.0.0.1:{node.Port}",
            Protocol = Protocol.Tcp,
            ReconnectionSettings = new ReconnectionSettings
            {
                Enabled = true,
                MaxRetries = 2,
                InitialDelay = TimeSpan.FromMilliseconds(20)
            }
        };
        using var client = new TcpMessageStream(configuration, NullLoggerFactory.Instance);

        await client.ConnectAsync(TestContext.Current.CancellationToken);
        await client.LoginUserAsync("iggy", "iggy", TestContext.Current.CancellationToken);
        await client.PingAsync(TestContext.Current.CancellationToken);
        var connectionsBeforeEviction = node.Connections;

        evict = true;
        var evicted = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() =>
            client.PingAsync(TestContext.Current.CancellationToken));
        Assert.Equal(VsrError.STALE_CLIENT, evicted.StatusCode);
        Assert.True(evicted.FromServer);
        Assert.Equal(connectionsBeforeEviction, node.Connections);

        // The dropped connection leaves the next call transport-shaped, but the eviction forgot the remembered
        // sign-in, so it must fail fast instead of reconnecting into a resurrected session.
        await Assert.ThrowsAsync<NotConnectedException>(() =>
            client.PingAsync(TestContext.Current.CancellationToken));
        Assert.Equal(connectionsBeforeEviction, node.Connections);
    }

    /// <summary>
    ///     The same contract when the eviction lands on a replicated write: that request is reported as
    ///     outcome-unknown rather than as a lost connection, so it never passes through the lost-connection
    ///     path, and the session it evicted still has to be forgotten.
    /// </summary>
    [Fact]
    public async Task ServerEvictionDuringAReplicatedWriteForgetsTheRememberedSignIn()
    {
        using var node = new MockNode();
        var evict = false;
        node.Serve(request =>
        {
            if (request.Operation == OperationRegister)
            {
                return Reply(OperationRegister, RegisterBody(session: 128));
            }

            return evict
                ? EvictionFrame(EvictionStaleClient)
                : Reply(OperationNonReplicated, request.Code == GetClusterMetadataCode
                    ? ClusterMetadata(node.Port, node.Port, node.Port)
                    : []);
        });

        var configuration = new IggyClientConfigurator
        {
            BaseAddress = $"127.0.0.1:{node.Port}",
            Protocol = Protocol.Tcp,
            ReconnectionSettings = new ReconnectionSettings
            {
                Enabled = true,
                MaxRetries = 2,
                InitialDelay = TimeSpan.FromMilliseconds(20)
            }
        };
        using var client = new TcpMessageStream(configuration, NullLoggerFactory.Instance);

        await client.ConnectAsync(TestContext.Current.CancellationToken);
        await client.LoginUserAsync("iggy", "iggy", TestContext.Current.CancellationToken);
        await client.PingAsync(TestContext.Current.CancellationToken);
        var connectionsBeforeEviction = node.Connections;

        evict = true;
        await Assert.ThrowsAsync<VsrRequestOutcomeUnknownException>(() =>
            client.CreateStreamAsync("evicted-mid-write", token: TestContext.Current.CancellationToken));
        Assert.Equal(connectionsBeforeEviction, node.Connections);

        await Assert.ThrowsAsync<NotConnectedException>(() =>
            client.PingAsync(TestContext.Current.CancellationToken));
        Assert.Equal(connectionsBeforeEviction, node.Connections);
    }

    /// <summary>
    ///     A survivor that only comes up after the first rotation still has to be found. The reconnection
    ///     budget counts rotations, not dials, so one retry is one full pass over every endpoint the client
    ///     knows rather than one dial of the endpoint it started from.
    /// </summary>
    [Fact]
    public async Task ResumesOnASurvivorThatComesUpAfterTheFirstRotation()
    {
        // A port nothing listens on yet: the survivor comes up on it only after the first rotation has
        // already failed on both endpoints.
        var probe = new TcpListener(IPAddress.Loopback, 0);
        probe.Start();
        var survivorPort = (ushort)((IPEndPoint)probe.LocalEndpoint).Port;
        probe.Stop();

        using var primary = new MockNode();
        primary.Serve(request => request.Code == GetClusterMetadataCode
            ? Reply(OperationNonReplicated, ClusterMetadata(primary.Port, survivorPort, primary.Port))
            : Answer(request));

        var configuration = new IggyClientConfigurator
        {
            BaseAddress = $"127.0.0.1:{primary.Port}",
            Protocol = Protocol.Tcp,
            AutoLoginSettings = new AutoLoginSettings { Enabled = true, Username = "iggy", Password = "iggy" },
            ReconnectionSettings = new ReconnectionSettings
            {
                Enabled = true,
                // One retry: the rotation after the first failure is the last one, which is where the
                // budget check used to cut the sweep short.
                MaxRetries = 1,
                InitialDelay = TimeSpan.FromMilliseconds(600)
            }
        };
        using var client = new TcpMessageStream(configuration, NullLoggerFactory.Instance);

        await client.ConnectAsync(TestContext.Current.CancellationToken);
        await client.LoginUserAsync("iggy", "iggy", TestContext.Current.CancellationToken);
        await client.PingAsync(TestContext.Current.CancellationToken);

        primary.Kill();
        using var survivor = new MockNode(survivorPort);
        var comesUp = Task.Run(async () =>
        {
            await Task.Delay(250, TestContext.Current.CancellationToken);
            survivor.Serve(request => request.Code == GetClusterMetadataCode
                ? Reply(OperationNonReplicated, ClusterMetadata(primary.Port, survivorPort, survivorPort))
                : Answer(request));
        }, TestContext.Current.CancellationToken);

        await client.PingAsync(TestContext.Current.CancellationToken);
        await comesUp;

        Assert.True(survivor.Registrations >= 1, "the session was re-established on the survivor");
    }

    private static byte[] EvictionFrame(byte reason)
    {
        var frame = new byte[HeaderSize];
        BinaryPrimitives.WriteUInt32LittleEndian(frame.AsSpan(SizeOffset, 4), HeaderSize);
        frame[CommandOffset] = CommandEviction;
        frame[EvictionReasonOffset] = reason;
        return frame;
    }

    [Fact]
    public async Task FailsFastWhenNothingEverSignedIn()
    {
        using var node = new MockNode();
        node.Serve(request => request.Code == GetClusterMetadataCode
            ? Reply(OperationNonReplicated, ClusterMetadata(node.Port, node.Port, node.Port))
            : Answer(request));

        var configuration = new IggyClientConfigurator
        {
            BaseAddress = $"127.0.0.1:{node.Port}",
            Protocol = Protocol.Tcp,
            ReconnectionSettings = new ReconnectionSettings
            {
                Enabled = true,
                MaxRetries = 2,
                InitialDelay = TimeSpan.FromMilliseconds(20)
            }
        };
        using var client = new TcpMessageStream(configuration, NullLoggerFactory.Instance);

        await client.ConnectAsync(TestContext.Current.CancellationToken);
        await client.PingAsync(TestContext.Current.CancellationToken);

        // A reconnect announces itself by entering Connecting, so the absence of that transition is the
        // assertion - no need to poll for a request that must never succeed.
        var reconnected = false;
        client.SubscribeConnectionEvents(args =>
        {
            reconnected |= args.CurrentState == ConnectionState.Connecting;
            return Task.CompletedTask;
        });

        node.Kill();

        await Assert.ThrowsAnyAsync<Exception>(() => client.PingAsync(TestContext.Current.CancellationToken));
        Assert.False(reconnected, "a client that never signed in cannot restore a session by reconnecting");
    }

    private static async Task<(bool Resumed, string LastError)> ResumedWithin(TcpMessageStream client,
        TimeSpan budget)
    {
        var deadline = DateTimeOffset.UtcNow + budget;
        var lastError = "none";
        var attempts = 0;
        while (DateTimeOffset.UtcNow < deadline)
        {
            attempts++;
            try
            {
                await client.PingAsync(TestContext.Current.CancellationToken);

                return (true, lastError);
            }
            catch (Exception error)
            {
                lastError = $"{attempts} attempts, last: {error.GetType().Name}: {error.Message}";
                await Task.Delay(50, TestContext.Current.CancellationToken);
            }
        }

        return (false, lastError);
    }

    /// <summary>A reply for anything the roster read does not claim: a register, or an empty read.</summary>
    private static byte[] Answer(MockRequest request)
    {
        return request.Operation == OperationRegister
            ? Reply(OperationRegister, RegisterBody(session: 128))
            : Reply(OperationNonReplicated, []);
    }

    private static byte[] Reply(byte operation, byte[] body)
    {
        var frame = new byte[HeaderSize + body.Length];
        BinaryPrimitives.WriteUInt32LittleEndian(frame.AsSpan(SizeOffset, 4), (uint)frame.Length);
        frame[CommandOffset] = CommandReply;
        frame[ReplyOperationOffset] = operation;
        BinaryPrimitives.WriteUInt32LittleEndian(frame.AsSpan(ReplyStatusOffset, 4), 0);
        body.CopyTo(frame.AsSpan(HeaderSize));

        return frame;
    }

    /// <summary>
    ///     A register reply carries a committed result section, so its four leading zero bytes announce zero
    ///     entries and the typed payload starts right after them. A non-replicated read carries none.
    /// </summary>
    private static byte[] RegisterBody(ulong session)
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

    private static byte[] ClusterMetadata(ushort primaryPort, ushort survivorPort, ushort leaderPort)
    {
        var body = new List<byte>();
        WriteString(body, "test-cluster");
        body.AddRange(BitConverter.GetBytes(2u));
        WriteNode(body, "primary", primaryPort, primaryPort == leaderPort);
        WriteNode(body, "survivor", survivorPort, survivorPort == leaderPort);

        return body.ToArray();
    }

    private static void WriteNode(List<byte> body, string name, ushort port, bool leader)
    {
        WriteString(body, name);
        WriteString(body, "127.0.0.1");
        body.AddRange(BitConverter.GetBytes(port));
        body.AddRange(BitConverter.GetBytes((ushort)0));
        body.AddRange(BitConverter.GetBytes((ushort)0));
        body.AddRange(BitConverter.GetBytes((ushort)0));
        body.Add(leader ? (byte)0 : (byte)1);
        body.Add(0);
    }

    private static void WriteString(List<byte> body, string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        body.AddRange(BitConverter.GetBytes((uint)bytes.Length));
        body.AddRange(bytes);
    }

    private readonly record struct MockRequest(byte Operation, int Code, ulong RequestId);

    /// <summary>
    ///     A loopback VSR node. Killing it drops the live sockets and stops accepting, so a redial is refused the
    ///     way a dead process refuses one.
    /// </summary>
    private sealed class MockNode : IDisposable
    {
        private readonly TcpListener _listener;
        private readonly List<TcpClient> _accepted = [];
        private volatile bool _killed;
        private int _connections;
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
                var header = new byte[HeaderSize];
                while (!_killed)
                {
                    await ReadExactly(stream, header);
                    var size = BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(SizeOffset, 4));
                    var body = new byte[size - HeaderSize];
                    await ReadExactly(stream, body);

                    var request = new MockRequest(header[RequestOperationOffset],
                        BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(RequestReservedOffset, 4)),
                        BinaryPrimitives.ReadUInt64LittleEndian(header.AsSpan(RequestIdOffset, 8)));
                    if (request.Operation == OperationRegister)
                    {
                        Interlocked.Increment(ref _registrations);
                    }
                    else if (request.Code == PingCode)
                    {
                        Interlocked.Increment(ref _pings);
                    }

                    var reply = handler(request);
                    BinaryPrimitives.WriteUInt64LittleEndian(reply.AsSpan(ReplyRequestIdOffset, 8),
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
}
