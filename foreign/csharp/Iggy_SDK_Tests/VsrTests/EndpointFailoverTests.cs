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
using Apache.Iggy.Configuration;
using Apache.Iggy.Contracts;
using Apache.Iggy.Contracts.Tcp;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.IggyClient;
using Apache.Iggy.IggyClient.Implementations;
using Apache.Iggy.Messages;
using Apache.Iggy.Utils;
using Apache.Iggy.Vsr;
using Microsoft.Extensions.Logging.Abstractions;
using static Apache.Iggy.Tests.VsrTests.MockFrames;

namespace Apache.Iggy.Tests.VsrTests;

/// <summary>
///     The node a client signed in on dies; its next request has to complete on a survivor the roster named,
///     under a session established there. Mirrors
///     <c>core/integration/tests/cluster/failover_client_continuity.rs</c>.
/// </summary>
public sealed class EndpointFailoverTests
{
    [Theory]
    [InlineData(VsrError.TRANSIENT_NOT_ACCEPTED)]
    [InlineData(VsrError.UNAUTHORIZED)]
    [InlineData(VsrError.UNAUTHENTICATED)]
    [InlineData(VsrError.STALE_CLIENT)]
    [InlineData(VsrError.NOT_PRIMARY)]
    [InlineData(VsrError.LIFECYCLE_BUSY)]
    [InlineData(VsrError.FEATURE_UNAVAILABLE)]
    public async Task LaterRefusalsKeepTheIdentityOfAnUncertainMetadataWrite(int refusal)
    {
        using var node = new MockNode();
        var attempts = new ConcurrentQueue<MockRequest>();
        var proveCommit = false;
        node.Serve(request =>
        {
            if (request.Operation == (byte)VsrOperation.CreatePartitions)
            {
                attempts.Enqueue(request);
                var status = proveCommit ? 0 : attempts.Count == 1 ? VsrError.TRANSIENT_NOT_COMMITTED : refusal;
                return Reply(request.Operation, status == 0 ? new byte[4] : [], (uint)status);
            }
            return request.Code == GET_CLUSTER_METADATA_CODE
                ? Reply(OPERATION_NON_REPLICATED, ClusterMetadata(node.Port, node.Port, node.Port))
                : Answer(request);
        });
        using var client = new TcpMessageStream(new IggyClientConfigurator
        {
            BaseAddress = $"127.0.0.1:{node.Port}",
            Protocol = Protocol.Tcp,
            HeartbeatInterval = TimeSpan.FromHours(1),
            ReconnectionSettings = new ReconnectionSettings { Enabled = true, InitialDelay = TimeSpan.Zero }
        }, NullLoggerFactory.Instance);
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        await client.LoginUserAsync("iggy", "iggy", TestContext.Current.CancellationToken);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cancellation.CancelAfter(TimeSpan.FromMilliseconds(500));
        var unknown = await Assert.ThrowsAsync<VsrRequestOutcomeUnknownException>(() =>
            client.CreatePartitionsAsync(Identifier.Numeric(1), Identifier.Numeric(1), 1, cancellation.Token));
        var id = Assert.IsType<ulong>(unknown.OperationId);
        Assert.True(attempts.Count >= 2);
        var original = attempts.First();
        Assert.All(attempts, replay =>
        {
            Assert.Equal(original.ClientId, replay.ClientId);
            Assert.Equal(original.Session, replay.Session);
            Assert.Equal(original.RequestId, replay.RequestId);
            Assert.Equal(original.Body, replay.Body);
        });
        proveCommit = true;
        if (refusal is VsrError.UNAUTHENTICATED or VsrError.STALE_CLIENT)
        {
            await UntilAsync(() => node.Registrations == 2);
            await Assert.ThrowsAsync<VsrOperationNotRetainedException>(() =>
                client.ResolveOperationAsync(id, TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken));
        }
        else
        {
            var count = attempts.Count;
            var blocked = await Assert.ThrowsAsync<VsrLaneBlockedException>(() =>
                client.CreatePartitionsAsync(Identifier.Numeric(1), Identifier.Numeric(1), 1,
                    TestContext.Current.CancellationToken));
            Assert.Equal(id, blocked.OperationId);
            Assert.Equal(count, attempts.Count);
            await client.ResolveOperationAsync(id, TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
            Assert.Equal(original.RequestId, attempts.Last().RequestId);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ResumesOnASurvivorAfterTheSignedInNodeDies(bool changeCredentials)
    {
        using var primary = new MockNode();
        using var survivor = new MockNode();

        primary.Serve(request => request.Code == GET_CLUSTER_METADATA_CODE
            ? Reply(OPERATION_NON_REPLICATED, ClusterMetadata(primary.Port, survivor.Port, primary.Port))
            : Answer(request));
        survivor.Serve(request =>
        {
            return request.Code == GET_CLUSTER_METADATA_CODE
                ? Reply(OPERATION_NON_REPLICATED, ClusterMetadata(primary.Port, survivor.Port, survivor.Port))
                : Answer(request);
        });

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
        if (changeCredentials)
        {
            await client.UpdateUserAsync(Identifier.Numeric(7), "renamed", token: TestContext.Current.CancellationToken);
            await client.ChangePasswordAsync(Identifier.String("renamed"), "iggy", "new-secret",
                TestContext.Current.CancellationToken);
        }
        await client.PingAsync(TestContext.Current.CancellationToken);
        Assert.Equal(1, primary.Pings);

        primary.Kill();

        // The request in flight when the node died is allowed to fail; what is not allowed is never completing
        // one, which is what a client that only knows the dead endpoint does.
        var (resumed, lastError) = await ResumedWithin(client, TimeSpan.FromSeconds(10));
        Assert.True(resumed,
            $"the client has to resume on the survivor the roster named ({lastError}, survivor saw " +
            $"{survivor.Registrations} registrations and {survivor.Pings} pings)");
        Assert.Equal(0, survivor.Registrations);
        Assert.True(survivor.Requests(BIND_SESSION_CODE) >= 1, "the retained session resumed on the survivor");
        Assert.True(survivor.Pings >= 1, "the request landed on the survivor");
    }

    [Theory]
    [InlineData(0)]
    [InlineData(VsrError.UNAUTHENTICATED)]
    [InlineData(VsrError.STALE_CLIENT)]
    [InlineData(VsrError.TRANSIENT_NOT_COMMITTED)]
    [InlineData(VsrError.TRANSIENT_NOT_ACCEPTED)]
    [InlineData(VsrError.SESSION_MISMATCH)]
    public async Task ReconnectUsesBindBeforeReplacingALogicalSession(int bindStatus)
    {
        const ulong FirstSession = 128;
        using var node = new MockNode();
        var registrations = new ConcurrentQueue<MockRequest>();
        var bindings = new ConcurrentQueue<MockRequest>();
        var writes = new ConcurrentQueue<MockRequest>();
        var pings = new ConcurrentQueue<MockRequest>();
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        node.Serve(request =>
        {
            if (request.Operation == OPERATION_REGISTER)
            {
                registrations.Enqueue(request);
                return Reply(request.Operation, RegisterBody(FirstSession + (ulong)registrations.Count - 1));
            }
            if (request.Code == BIND_SESSION_CODE)
            {
                bindings.Enqueue(request);
                if (bindings.Count == 1 && bindStatus == VsrError.SESSION_MISMATCH)
                {
                    return Reply(request.Operation, RegisterBody(request.Session + 1)[4..]);
                }
                return bindings.Count == 1 && bindStatus != 0
                    ? Reply(request.Operation, [], (uint)bindStatus)
                    : Answer(request);
            }
            if (request.Operation == (byte)VsrOperation.CreateStream)
            {
                writes.Enqueue(request);
                cancellation.Cancel();
                throw new IOException("The connection lost the committed write reply.");
            }
            if (request.Code == PING_CODE)
            {
                pings.Enqueue(request);
            }
            return request.Code switch
            {
                GET_CLUSTER_METADATA_CODE => Reply(OPERATION_NON_REPLICATED, ClusterMetadata(node.Port, node.Port, node.Port)),
                CommandCodes.GET_PARTITION_ROUTE_CODE => RouteReply(request, new PartitionContext(17, 0, 1), 0),
                _ => Answer(request)
            };
        });
        using var client = new TcpMessageStream(new IggyClientConfigurator
        {
            BaseAddress = $"127.0.0.1:{node.Port}",
            Protocol = Protocol.Tcp,
            HeartbeatInterval = TimeSpan.FromHours(1),
            ReconnectionSettings = new ReconnectionSettings
            {
                Enabled = true,
                MaxRetries = 4,
                InitialDelay = TimeSpan.Zero,
                WaitAfterReconnect = TimeSpan.Zero
            }
        }, NullLoggerFactory.Instance);
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        await client.LoginUserAsync("iggy", "iggy", TestContext.Current.CancellationToken);
        var generation = ((ISessionGenerationProvider)client).SessionGeneration;
        var registration = Assert.Single(registrations);

        await Assert.ThrowsAsync<VsrRequestOutcomeUnknownException>(() =>
            client.CreateStreamAsync("lost-reply", token: cancellation.Token));
        var write = Assert.Single(writes);
        Assert.Equal(registration.ClientId, write.ClientId);

        if (bindStatus == VsrError.SESSION_MISMATCH)
        {
            await UntilAsync(() => bindings.Count == 1);
            var refusal = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() =>
                client.PingAsync(TestContext.Current.CancellationToken));
            Assert.Equal(bindStatus, refusal.StatusCode);
            Assert.Single(registrations);
            Assert.Equal(generation, ((ISessionGenerationProvider)client).SessionGeneration);
            await client.ConnectAsync(TestContext.Current.CancellationToken);
        }
        var (resumed, lastError) = await ResumedWithin(client, TimeSpan.FromSeconds(5));
        Assert.True(resumed, lastError);

        Assert.Single(writes);
        Assert.NotEmpty(bindings);
        Assert.All(bindings.Where(binding => binding.ClientId == registration.ClientId), binding =>
        {
            Assert.Equal(registration.ClientId, binding.ClientId);
            Assert.Equal(write.Session, binding.Session);
            Assert.Equal(LoginRegister.ReadBindSecret(registration.Body),
                binding.Body.AsSpan(binding.Body.Length - LoginRegister.BIND_SECRET_BYTES).ToArray());
        });
        var ping = pings.Last();
        if (bindStatus is VsrError.UNAUTHENTICATED or VsrError.STALE_CLIENT)
        {
            Assert.Equal(2, registrations.Count);
            Assert.NotEqual(registration.ClientId, ping.ClientId);
            Assert.NotEqual(write.Session, ping.Session);
            Assert.NotEqual(generation, ((ISessionGenerationProvider)client).SessionGeneration);
        }
        else
        {
            Assert.Single(registrations);
            Assert.Equal(write.ClientId, ping.ClientId);
            Assert.Equal(write.Session, ping.Session);
            Assert.Equal(generation, ((ISessionGenerationProvider)client).SessionGeneration);
        }
    }

    [Theory]
    [InlineData(3)]
    [InlineData(5)]
    public async Task RoutesPartitionWritesToTheAdvertisedPrimaryWithoutWalkingReplicas(int replicaCount)
    {
        var nodes = Enumerable.Range(0, replicaCount).Select(_ => new MockNode()).ToArray();
        try
        {
            var roster = nodes.Select(node => node.Port).ToArray();
            var writes = new int[nodes.Length];
            for (var index = 0; index < nodes.Length; index++)
            {
                var replica = index;
                nodes[index].Serve(request =>
                {
                    if (request.Operation == (byte)VsrOperation.StoreConsumerOffset)
                    {
                        Interlocked.Increment(ref writes[replica]);
                        return replica != nodes.Length - 1
                            ? Reply(request.Operation, [], VsrError.TRANSIENT_NOT_ACCEPTED) : Answer(request);
                    }
                    return request.Code switch
                    {
                        GET_CLUSTER_METADATA_CODE => Reply(request.Operation, RosterMetadata(nodes[0].Port, roster)),
                        CommandCodes.GET_PARTITION_ROUTE_CODE => RouteReply(request, new PartitionContext(17, 0, 1),
                            checked((byte)(nodes.Length - 1))),
                        _ => Answer(request)
                    };
                });
            }
            using var client = new TcpMessageStream(new IggyClientConfigurator
            {
                BaseAddress = $"127.0.0.1:{nodes[0].Port}",
                Protocol = Protocol.Tcp,
                HeartbeatInterval = TimeSpan.FromHours(1)
            }, NullLoggerFactory.Instance);
            await client.ConnectAsync(TestContext.Current.CancellationToken);
            await client.LoginUserAsync("iggy", "iggy", TestContext.Current.CancellationToken);
            await client.StoreOffsetAsync(Kinds.Consumer.New(3), Identifier.Numeric(1), Identifier.Numeric(2),
                10, 0, TestContext.Current.CancellationToken);
            Assert.All(writes[..^1], count => Assert.Equal(0, count));
            Assert.Equal(1, writes[^1]);
            await UntilAsync(() => nodes.All(node => node.Connections == 1));
            Assert.All(nodes[1..], node => Assert.Equal(0, node.Registrations));
        }
        finally
        {
            foreach (var node in nodes)
            {
                node.Dispose();
            }
        }
    }

    /// <summary>
    ///     Mirrors the integration contract (HeartbeatTests
    ///     EvictedClient_WithoutAutoLogin_Should_ReestablishItsSession): an eviction comes off the server's
    ///     heartbeat timer rather than from the caller, so the sign-in this client remembered survives it and
    ///     the reconnect re-establishes the session. Same rule in every SDK.
    /// </summary>
    [Fact]
    public async Task ServerEvictionReplaysTheRememberedSignIn()
    {
        using var node = new MockNode();
        var evict = false;
        node.Serve(request =>
        {
            if (request.Operation == OPERATION_REGISTER)
            {
                return Reply(OPERATION_REGISTER, RegisterBody(session: 128));
            }

            if (request.Code == BIND_SESSION_CODE)
            {
                return Reply(OPERATION_NON_REPLICATED, [], VsrError.UNAUTHENTICATED);
            }

            if (evict)
            {
                evict = false;
                return EvictionFrame(EVICTION_STALE_CLIENT);
            }

            return Reply(OPERATION_NON_REPLICATED, request.Code == GET_CLUSTER_METADATA_CODE
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
        var registrationsBeforeEviction = node.Registrations;

        evict = true;
        await Assert.ThrowsAnyAsync<Exception>(() => client.PingAsync(TestContext.Current.CancellationToken));
        var (resumed, lastError) = await ResumedWithin(client, TimeSpan.FromSeconds(5));
        Assert.True(resumed, lastError);

        Assert.True(node.Registrations > registrationsBeforeEviction,
            "the reconnect signed in again with the remembered credentials");
        await client.PingAsync(TestContext.Current.CancellationToken);
    }

    /// <summary>
    ///     The same rule when the eviction lands on a replicated write: that request is reported as
    ///     outcome-unknown, because its own outcome is unknown, but the session behind it is still
    ///     re-established for the requests that follow.
    /// </summary>
    [Fact]
    public async Task ServerEvictionDuringAReplicatedWriteReplaysTheRememberedSignIn()
    {
        using var node = new MockNode();
        var evict = false;
        node.Serve(request =>
        {
            if (request.Operation == OPERATION_REGISTER)
            {
                return Reply(OPERATION_REGISTER, RegisterBody(session: 128));
            }

            if (request.Code == BIND_SESSION_CODE)
            {
                return Reply(OPERATION_NON_REPLICATED, [], VsrError.UNAUTHENTICATED);
            }

            if (evict)
            {
                evict = false;
                return EvictionFrame(EVICTION_STALE_CLIENT);
            }

            return Reply(OPERATION_NON_REPLICATED, request.Code == GET_CLUSTER_METADATA_CODE
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
        var registrationsBeforeEviction = node.Registrations;

        evict = true;
        await Assert.ThrowsAsync<VsrRequestOutcomeUnknownException>(() =>
            client.CreateStreamAsync("evicted-mid-write", token: TestContext.Current.CancellationToken));

        var (resumed, lastError) = await ResumedWithin(client, TimeSpan.FromSeconds(5));
        Assert.True(resumed, lastError);
        Assert.True(node.Registrations > registrationsBeforeEviction,
            "the reconnect signed in again with the remembered credentials");
    }

    [Fact]
    public async Task LostOffsetReplyDoesNotReplayOverAnotherClientsProgress()
    {
        const long FirstOffset = 10;
        const long AdvancedOffset = 20;
        const int OffsetAndAckSize = sizeof(long) + sizeof(byte);
        using var node = new MockNode();
        using var advanced = new ManualResetEventSlim();
        var committed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var writes = 0;
        var applied = new ConcurrentDictionary<(UInt128 Client, ulong Request), byte>();
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        long storedOffset = 0;
        node.Serve(request =>
        {
            if (request.Operation == (byte)VsrOperation.StoreConsumerOffset)
            {
                var attempt = Interlocked.Increment(ref writes);
                if (!applied.TryAdd((request.ClientId, request.RequestId), 0))
                {
                    return Reply(request.Operation, new byte[4]);
                }
                var offset = BinaryPrimitives.ReadInt64LittleEndian(
                    request.Body.AsSpan(request.Body.Length - OffsetAndAckSize, sizeof(long)));
                Interlocked.Exchange(ref storedOffset, offset);
                if (attempt == 1)
                {
                    cancellation.Cancel();
                    committed.SetResult();
                    if (!advanced.Wait(TimeSpan.FromSeconds(5)))
                    {
                        throw new TimeoutException("The second client did not advance the offset.");
                    }
                    return [];
                }
                advanced.Set();
                return Reply(request.Operation, new byte[4]);
            }
            return request.Code switch
            {
                GET_CLUSTER_METADATA_CODE => Reply(OPERATION_NON_REPLICATED,
                    ClusterMetadata(node.Port, node.Port, node.Port)),
                CommandCodes.GET_PARTITION_ROUTE_CODE => RouteReply(request, new PartitionContext(17, 0, 1), 0),
                _ => Answer(request)
            };
        });
        var configuration = new IggyClientConfigurator
        {
            BaseAddress = $"127.0.0.1:{node.Port}",
            Protocol = Protocol.Tcp,
            HeartbeatInterval = TimeSpan.FromHours(1),
            ReconnectionSettings = new ReconnectionSettings
            {
                Enabled = true,
                MaxRetries = 1,
                InitialDelay = TimeSpan.Zero,
                WaitAfterReconnect = TimeSpan.Zero
            }
        };
        using var first = new TcpMessageStream(configuration, NullLoggerFactory.Instance);
        using var second = new TcpMessageStream(configuration, NullLoggerFactory.Instance);
        await first.ConnectAsync(TestContext.Current.CancellationToken);
        await first.LoginUserAsync("iggy", "iggy", TestContext.Current.CancellationToken);
        await second.ConnectAsync(TestContext.Current.CancellationToken);
        await second.LoginUserAsync("iggy", "iggy", TestContext.Current.CancellationToken);
        var pending = first.StoreOffsetAsync(Kinds.Consumer.New(3), Identifier.Numeric(1), Identifier.Numeric(2),
            FirstOffset, 0, cancellation.Token);
        try
        {
            await committed.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            var unknown = await Assert.ThrowsAsync<VsrRequestOutcomeUnknownException>(() => pending);
            var id = Assert.IsType<ulong>(unknown.OperationId);
            Assert.Equal(FirstOffset, Volatile.Read(ref storedOffset));
            await second.StoreOffsetAsync(Kinds.Consumer.New(3), Identifier.Numeric(1), Identifier.Numeric(2),
                AdvancedOffset, 0, TestContext.Current.CancellationToken);
            await first.ResolveOperationAsync(id, TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            Assert.Equal(3, Volatile.Read(ref writes));
            Assert.Equal(2, applied.Count);
            Assert.Equal(AdvancedOffset, Volatile.Read(ref storedOffset));
        }
        finally
        {
            advanced.Set();
        }
    }

    /// <summary>
    ///     REQUEST_TOO_OLD refuses a write whose request id fell below the server's deduplication window. The
    ///     server no longer knows whether it committed, so the caller gets the outcome-unknown type that the
    ///     publisher refuses to resend, the same stop the Rust producer makes on this code.
    /// </summary>
    [Fact]
    public async Task AgedOutRefusalOfAWriteIsReportedAsOutcomeUnknown()
    {
        using var node = new MockNode();
        var sends = 0;
        node.Serve(request =>
        {
            if (request.Operation == (byte)VsrOperation.SendMessages)
            {
                Interlocked.Increment(ref sends);

                return Reply(request.Operation, [], VsrError.REQUEST_TOO_OLD);
            }

            return request.Code switch
            {
                GET_CLUSTER_METADATA_CODE => Reply(OPERATION_NON_REPLICATED, ClusterMetadata(node.Port, node.Port, node.Port)),
                CommandCodes.GET_PARTITION_ROUTE_CODE => RouteReply(request, new PartitionContext(17, 0, 1), 0),
                _ => Answer(request)
            };
        });

        var configuration = new IggyClientConfigurator
        {
            BaseAddress = $"127.0.0.1:{node.Port}",
            Protocol = Protocol.Tcp
        };
        using var client = new TcpMessageStream(configuration, NullLoggerFactory.Instance);

        await client.ConnectAsync(TestContext.Current.CancellationToken);
        await client.LoginUserAsync("iggy", "iggy", TestContext.Current.CancellationToken);

        var unknown = await Assert.ThrowsAsync<VsrRequestOutcomeUnknownException>(() =>
            client.SendMessagesAsync(Identifier.Numeric(1), Identifier.Numeric(1),
                Kinds.Partitioning.PartitionId(1), new Message(Guid.NewGuid(), new byte[] { 1 }),
                TestContext.Current.CancellationToken));

        var refusal = Assert.IsType<IggyInvalidStatusCodeException>(unknown.InnerException);
        Assert.Equal(VsrError.REQUEST_TOO_OLD, refusal.StatusCode);
        Assert.Equal(1, Volatile.Read(ref sends));
    }

    [Fact]
    public async Task ResumesOnASurvivorThatComesUpWhileTheClientIsRetrying()
    {
        // A port nothing listens on: the survivor binds it only after the client has already failed on both
        // endpoints, so the first pass cannot be the one that finds it.
        var probe = new TcpListener(IPAddress.Loopback, 0);
        probe.Start();
        var survivorPort = (ushort)((IPEndPoint)probe.LocalEndpoint).Port;
        probe.Stop();

        using var primary = new MockNode();
        primary.Serve(request => request.Code == GET_CLUSTER_METADATA_CODE
            ? Reply(OPERATION_NON_REPLICATED, ClusterMetadata(primary.Port, survivorPort, primary.Port))
            : Answer(request));

        var configuration = new IggyClientConfigurator
        {
            BaseAddress = $"127.0.0.1:{primary.Port}",
            Protocol = Protocol.Tcp,
            AutoLoginSettings = new AutoLoginSettings { Enabled = true, Username = "iggy", Password = "iggy" },
            ReconnectionSettings = new ReconnectionSettings
            {
                Enabled = true,
                MaxRetries = 0,
                InitialDelay = TimeSpan.FromMilliseconds(200)
            }
        };
        using var client = new TcpMessageStream(configuration, NullLoggerFactory.Instance);

        await client.ConnectAsync(TestContext.Current.CancellationToken);
        await client.LoginUserAsync("iggy", "iggy", TestContext.Current.CancellationToken);
        await client.PingAsync(TestContext.Current.CancellationToken);

        primary.Kill();
        MockNode? survivor = null;
        // Constructed inside the delay, because the listener starts in the constructor: built up front, the
        // survivor would be answering from the very first dial and nothing about the later passes would be
        // exercised.
        var comesUp = Task.Run(async () =>
        {
            await Task.Delay(300, TestContext.Current.CancellationToken);
            survivor = new MockNode(survivorPort);
            survivor.Serve(request => request.Code == GET_CLUSTER_METADATA_CODE
                ? Reply(OPERATION_NON_REPLICATED, ClusterMetadata(primary.Port, survivorPort, survivorPort))
                : Answer(request));
        }, TestContext.Current.CancellationToken);

        try
        {
            var (resumed, lastError) = await ResumedWithin(client, TimeSpan.FromSeconds(5));
            Assert.True(resumed, lastError);
            await comesUp;

            Assert.NotNull(survivor);
            Assert.Equal(0, survivor!.Registrations);
            Assert.True(survivor.Requests(BIND_SESSION_CODE) >= 1, "the session resumed on the survivor");
        }
        finally
        {
            await comesUp;
            survivor?.Dispose();
        }
    }

    private static byte[] EvictionFrame(byte reason)
    {
        var frame = new byte[HEADER_SIZE];
        BinaryPrimitives.WriteUInt32LittleEndian(frame.AsSpan(SIZE_OFFSET, 4), HEADER_SIZE);
        frame[COMMAND_OFFSET] = COMMAND_EVICTION;
        frame[EVICTION_REASON_OFFSET] = reason;
        return frame;
    }

    [Fact]
    public async Task FailsFastWhenNothingEverSignedIn()
    {
        using var node = new MockNode();
        node.Serve(request => request.Code == GET_CLUSTER_METADATA_CODE
            ? Reply(OPERATION_NON_REPLICATED, ClusterMetadata(node.Port, node.Port, node.Port))
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
                using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
                cancellation.CancelAfter(deadline - DateTimeOffset.UtcNow);
                await client.PingAsync(cancellation.Token);

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

    private static async Task UntilAsync(Func<bool> predicate)
    {
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cancellation.CancelAfter(TimeSpan.FromSeconds(5));
        while (!predicate())
        {
            await Task.Delay(10, cancellation.Token);
        }
    }

    private static byte[] ClusterMetadata(ushort primaryPort, ushort survivorPort, ushort leaderPort)
    {
        var body = new List<byte>();
        WriteString(body, "test-cluster");
        body.AddRange(BitConverter.GetBytes(primaryPort == survivorPort ? 1u : 2u));
        WriteNode(body, "primary", primaryPort, primaryPort == leaderPort, 0);
        if (primaryPort != survivorPort)
        {
            WriteNode(body, "survivor", survivorPort, survivorPort == leaderPort, 1);
        }

        return body.ToArray();
    }

    private static byte[] RosterMetadata(ushort leaderPort, IReadOnlyList<ushort> ports)
    {
        var body = new List<byte>();
        WriteString(body, "test-cluster");
        body.AddRange(BitConverter.GetBytes((uint)ports.Count));
        for (var index = 0; index < ports.Count; index++)
        {
            WriteNode(body, $"node-{index}", ports[index], ports[index] == leaderPort, checked((byte)index));
        }

        return body.ToArray();
    }

    private static void WriteNode(List<byte> body, string name, ushort port, bool leader, byte replicaId)
    {
        body.Add(replicaId);
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
}
