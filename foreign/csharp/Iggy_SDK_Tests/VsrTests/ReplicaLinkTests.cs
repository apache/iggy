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

using System.Collections.Concurrent;
using Apache.Iggy.Configuration;
using Apache.Iggy.Contracts;
using Apache.Iggy.Enums;
using Apache.Iggy.IggyClient.Implementations;
using Apache.Iggy.Kinds;
using Apache.Iggy.Utils;
using Apache.Iggy.Vsr;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using static Apache.Iggy.Tests.VsrTests.MockFrames;

namespace Apache.Iggy.Tests.VsrTests;

/// <summary>
///     The background replica links of the TCP client. Mirrors the slot tests of
///     <c>core/sdk/src/links/replicas.rs</c>.
/// </summary>
public sealed class ReplicaLinkTests
{
    private const int MaxRetries = 1;
    private const int WaitMs = 5_000;
    private const int MeasuredRestarts = 4;
    private const int BindsPerRestart = 2;
    private static readonly TimeSpan Heartbeat = TimeSpan.FromMilliseconds(20);
    private static readonly TimeSpan CadenceHeartbeat = TimeSpan.FromMilliseconds(150);
    private static readonly Identifier Stream = Identifier.Numeric(1);
    private static readonly Identifier Topic = Identifier.Numeric(2);
    private static readonly uint[] Groups = [3, 4];

    [Fact]
    public async Task given_an_outage_longer_than_the_retry_budget_when_the_node_returns_should_resume_on_its_own()
    {
        var token = TestContext.Current.CancellationToken;
        // Nothing listens on the advertised port, so every bind below comes from a dial to the configured endpoint.
        var advertisedPort = UnusedPort();
        var down = false;
        using var node = new MockNode();
        node.Serve(request => Volatile.Read(ref down) ? throw new IOException("outage")
            : request.Code == GET_CLUSTER_METADATA_CODE ? StandaloneRoster(advertisedPort) : Answer(request));
        using var client = await SignedInAsync(node.Port);
        var states = RecordStates(client);
        int Binds() => node.Requests(BIND_SESSION_CODE);

        Volatile.Write(ref down, true);
        // The redial after the drop binds once and uses up the budget, so a third bind proves a restart.
        Assert.True(await EventuallyAsync(() => Binds() >= 3),
            $"The heartbeat restarts a link that used up its retry budget. Binds: {Binds()}.");
        Assert.Contains(ConnectionState.Disconnected, states);
        Volatile.Write(ref down, false);

        Assert.True(await EventuallyAsync(() => states.LastOrDefault() == ConnectionState.Authenticated),
            "The client resumes its session without an application call.");
        await client.PingAsync(token);
        Assert.Equal(1, node.Registrations);
    }

    [Fact]
    public async Task given_a_malformed_bind_reply_when_the_node_recovers_should_retry_the_slot()
    {
        using var node = new MockNode();
        var drop = 0;
        var binds = 0;
        node.Serve(request =>
        {
            if (request.Code == PING_CODE && Interlocked.Exchange(ref drop, 0) == 1)
            {
                throw new IOException("lost link");
            }
            if (request.Code == BIND_SESSION_CODE && Interlocked.Increment(ref binds) == 1)
            {
                return Reply(OPERATION_NON_REPLICATED, [0]);
            }
            return request.Code == GET_CLUSTER_METADATA_CODE ? StandaloneRoster(node.Port) : Answer(request);
        });
        using var client = await SignedInAsync(node.Port);
        Volatile.Write(ref drop, 1);

        Assert.True(await EventuallyAsync(() => Volatile.Read(ref binds) >= 2),
            "A malformed bind reply must not leave the replica slot stopped.");
        await client.PingAsync(TestContext.Current.CancellationToken);
        Assert.Equal(1, node.Registrations);
    }

    [Fact]
    public async Task given_one_link_out_of_retries_among_live_ones_when_the_heartbeat_ticks_should_restart_it()
    {
        var token = TestContext.Current.CancellationToken;
        using var cluster = new LinkCluster();
        using var client = await SignedInAsync(cluster.Coordinator.Port);
        int Binds() => cluster.Primary.Requests(BIND_SESSION_CODE);
        Assert.True(await EventuallyAsync(() => Binds() == 1), "The partition primary link binds the session.");

        cluster.PrimaryDown = true;
        // One redial after the drop, then two dials per restart, so a fourth bind proves a restart.
        Assert.True(await EventuallyAsync(() => Binds() >= 4),
            $"The heartbeat restarts a link that used up its retry budget. Binds: {Binds()}.");
        cluster.PrimaryDown = false;
        await client.StoreOffsetAsync(Consumer.New(3), Stream, Topic, 10, 0, token);

        Assert.Equal(1, Volatile.Read(ref cluster.PrimaryWrites));
        Assert.Equal(1, cluster.Coordinator.Registrations);
        Assert.Equal(0, cluster.Primary.Registrations);
    }

    [Fact]
    public async Task given_a_disconnected_member_of_two_groups_when_the_heartbeat_ticks_should_restart_links_without_syncing_groups()
    {
        using var cluster = new LinkCluster();
        var logger = new Mock<ILogger>();
        var loggers = new Mock<ILoggerFactory>();
        loggers.Setup(factory => factory.CreateLogger(It.IsAny<string>())).Returns(logger.Object);
        using var client = await InGroupsAsync(cluster, loggers.Object);
        var states = RecordStates(client);
        int Binds() => cluster.Primary.Requests(BIND_SESSION_CODE);
        // A failed group sync is the only warning a client without a ready link logs.
        int Warnings() => logger.Invocations.Count(call => call.Method.Name == nameof(ILogger.Log)
            && call.Arguments[0] is LogLevel.Warning);
        cluster.PrimaryDown = true;
        Assert.True(await EventuallyAsync(() => Binds() >= 2), "The partition primary link uses up its retry budget.");

        cluster.Coordinator.Kill();
        Assert.True(await EventuallyAsync(() => states.Contains(ConnectionState.Disconnected)),
            "The metadata primary link uses up its retry budget too.");
        var binds = Binds();
        // The tick that restarts the link begins after every group sync of an earlier tick has ended.
        Assert.True(await EventuallyAsync(() => Binds() > binds),
            $"Group syncs with no link to reach must not hold up link restarts. Binds: {Binds()}.");
        var warnings = Warnings();
        binds = Binds();

        // Two dials per restart, so eight more binds prove four more restarts.
        Assert.True(await EventuallyAsync(() => Binds() >= binds + 8),
            $"The heartbeat keeps restarting links while disconnected. Binds: {Binds()}.");
        Assert.Equal(warnings, Warnings());
    }

    [Fact]
    public async Task given_a_coordinator_holding_group_syncs_when_the_heartbeat_ticks_should_keep_restarting_links()
    {
        using var cluster = new LinkCluster();
        using var client = await InGroupsAsync(cluster);
        int Binds() => cluster.Primary.Requests(BIND_SESSION_CODE);
        cluster.PrimaryDown = true;
        Assert.True(await EventuallyAsync(() => Binds() >= 2), "The partition primary link uses up its retry budget.");

        cluster.SyncsHeld = true;
        var binds = Binds();

        // Two dials per restart, so eight more binds prove four restarts.
        Assert.True(await EventuallyAsync(() => Binds() >= binds + 8),
            $"Group syncs the coordinator never answers must not hold up link restarts. Binds: {Binds()}.");
    }

    [Fact]
    public async Task given_a_coordinator_holding_the_syncs_of_three_groups_when_the_heartbeat_ticks_should_restart_links_every_tick()
    {
        using var cluster = new LinkCluster();
        var failures = 0;
        var logger = new Mock<ILogger>();
        logger.Setup(log => log.IsEnabled(It.IsAny<LogLevel>())).Returns(true);
        logger.Setup(log => log.Log(It.IsAny<LogLevel>(), It.IsAny<EventId>(),
                It.IsAny<It.IsAnyType>(), It.IsAny<Exception?>(), It.IsAny<Func<It.IsAnyType, Exception?, string>>()))
            .Callback(new InvocationAction(call =>
            {
                if (call.Arguments[2]?.ToString()?.StartsWith("Failed to refresh the consumer group assignment", StringComparison.Ordinal) == true)
                {
                    Interlocked.Increment(ref failures);
                }
            }));
        var loggers = new Mock<ILoggerFactory>();
        loggers.Setup(factory => factory.CreateLogger(It.IsAny<string>())).Returns(logger.Object);
        using var client = await InGroupsAsync(cluster, loggers.Object, groups: [3, 4, 5], heartbeat: CadenceHeartbeat);
        int Binds() => cluster.Primary.Requests(BIND_SESSION_CODE);
        cluster.PrimaryDown = true;
        Assert.True(await EventuallyAsync(() => Binds() >= 2), "The partition primary link uses up its retry budget.");
        cluster.SyncsHeld = true;
        var observations = new ConcurrentQueue<int>();
        cluster.OnPrimaryBind = () => observations.Enqueue(Volatile.Read(ref failures));
        Assert.True(await EventuallyAsync(() => observations.Count >= BindsPerRestart * (MeasuredRestarts + 1)),
            $"Binds: {Binds()}.");
        var restarts = observations.ToArray().Where((_, index) => index % BindsPerRestart == 0).ToArray();
        for (var index = 1; index < restarts.Length; index++)
        {
            Assert.InRange(restarts[index] - restarts[index - 1], 0, 1);
        }
        Assert.True(restarts[^1] > restarts[0], "At least one held sync exhausted the shared refresh budget.");
    }

    [Fact]
    public async Task given_reconnection_disabled_when_a_link_drops_should_keep_it_down()
    {
        using var cluster = new LinkCluster();
        using var client = await SignedInAsync(cluster.Coordinator.Port, reconnect: false);
        Assert.True(await EventuallyAsync(() => cluster.Primary.Requests(BIND_SESSION_CODE) == 1),
            "The partition primary link binds the session.");

        cluster.PrimaryDown = true;
        Assert.True(await EventuallyAsync(() => cluster.Primary.ActiveConnections == 0),
            "The partition primary link drops.");
        cluster.PrimaryDown = false;
        var pings = cluster.Coordinator.Pings;
        Assert.True(await EventuallyAsync(() => cluster.Coordinator.Pings >= pings + 3),
            "The heartbeat keeps running on the live link.");

        Assert.Equal(1, cluster.Primary.Connections);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task given_a_link_out_of_retries_when_the_client_logs_out_or_is_disposed_should_never_redial_it(
        bool logout)
    {
        var token = TestContext.Current.CancellationToken;
        using var cluster = new LinkCluster();
        using var client = await SignedInAsync(cluster.Coordinator.Port);
        cluster.PrimaryDown = true;
        Assert.True(await EventuallyAsync(() => cluster.Primary.Requests(BIND_SESSION_CODE) >= 2),
            "The partition primary link uses up its retry budget.");
        // Refused from here on, so only a dial after the sign-out can reach the replacement.
        cluster.Primary.Kill();

        if (logout)
        {
            await client.LogoutUserAsync(token);
        }
        else
        {
            client.Dispose();
        }
        using var replacement = cluster.ReplacePrimary();
        await Task.Delay(Heartbeat * 10, token);

        Assert.Equal(0, replacement.Connections);
    }

    [Fact]
    public async Task given_a_single_node_advertising_another_address_when_its_link_drops_should_redial_the_configured_endpoint()
    {
        var token = TestContext.Current.CancellationToken;
        // A port mapping in front of the node, which advertises a port that nothing listens on.
        var advertisedPort = UnusedPort();
        var drop = 0;
        using var node = new MockNode();
        node.Serve(request => request.Code == CommandCodes.GET_STATS_CODE && Interlocked.Exchange(ref drop, 0) == 1
            ? throw new IOException("drop")
            : request.Code == GET_CLUSTER_METADATA_CODE ? StandaloneRoster(advertisedPort) : Answer(request));
        using var client = await SignedInAsync(node.Port);
        var configured = $"127.0.0.1:{node.Port}";

        Assert.Equal(configured, client.GetCurrentAddress());
        await client.GetStatsAsync(token);
        Assert.Equal(1, node.Connections);

        Volatile.Write(ref drop, 1);
        await client.GetStatsAsync(token);

        Assert.Equal(2, node.Connections);
        Assert.Equal(1, node.Requests(BIND_SESSION_CODE));
        Assert.Equal(1, node.Registrations);
        Assert.Equal(configured, client.GetCurrentAddress());
    }

    [Fact]
    public async Task given_a_single_node_when_a_reconnect_reaches_it_through_its_advertised_address_should_adopt_that_link()
    {
        var token = TestContext.Current.CancellationToken;
        using var advertised = new MockNode();
        using var configured = new MockNode();
        byte[] Handle(MockRequest request) => request.Code == GET_CLUSTER_METADATA_CODE
            ? StandaloneRoster(advertised.Port) : Answer(request);
        advertised.Serve(Handle);
        configured.Serve(Handle);
        using var client = await SignedInAsync(configured.Port);
        var states = RecordStates(client);

        // The configured endpoint stays down, so the reconnect falls back to the advertised address.
        configured.Kill();
        Assert.True(await EventuallyAsync(() => states.Contains(ConnectionState.Disconnected)),
            "The client loses its link to the configured endpoint.");
        await client.ConnectAsync(token);
        await client.GetStatsAsync(token);

        Assert.Equal(1, advertised.Connections);
    }

    [Fact]
    public async Task given_a_single_node_when_the_base_address_changes_after_connect_should_keep_the_configured_endpoint()
    {
        var token = TestContext.Current.CancellationToken;
        using var node = new MockNode();
        using var moved = new MockNode();
        node.Serve(request => request.Code == GET_CLUSTER_METADATA_CODE ? StandaloneRoster(node.Port) : Answer(request));
        moved.Serve(Answer);
        var configuration = Configuration(node.Port);
        var configured = configuration.BaseAddress;
        using var client = await SignedInAsync(configuration);

        configuration.BaseAddress = $"127.0.0.1:{moved.Port}";
        await client.GetClusterMetadataAsync(token);
        await client.GetStatsAsync(token);

        Assert.Equal(configured, client.GetCurrentAddress());
        Assert.Equal(1, node.Connections);
        Assert.Equal(0, moved.Connections);
    }

    private static Task<TcpMessageStream> SignedInAsync(ushort port, bool reconnect = true)
    {
        return SignedInAsync(Configuration(port, reconnect));
    }

    private static async Task<TcpMessageStream> SignedInAsync(IggyClientConfigurator configuration,
        ILoggerFactory? loggers = null)
    {
        var client = new TcpMessageStream(configuration, loggers ?? NullLoggerFactory.Instance);
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        await client.LoginUserAsync("iggy", "iggy", TestContext.Current.CancellationToken);
        return client;
    }

    /// <summary>
    ///     A signed-in client that a group poll made a member of every group in <paramref name="groups" />, by
    ///     default <see cref="Groups" />.
    /// </summary>
    private static async Task<TcpMessageStream> InGroupsAsync(LinkCluster cluster, ILoggerFactory? loggers = null,
        uint[]? groups = null, TimeSpan? heartbeat = null)
    {
        var client = await SignedInAsync(Configuration(cluster.Coordinator.Port, heartbeat: heartbeat), loggers);
        foreach (var group in groups ?? Groups)
        {
            await client.PollMessagesAsync(Stream, Topic, null, Consumer.Group(group), PollingStrategy.Next(), 1,
                false, TestContext.Current.CancellationToken);
        }
        return client;
    }

    private static IggyClientConfigurator Configuration(ushort port, bool reconnect = true,
        TimeSpan? heartbeat = null) => new()
        {
            BaseAddress = $"127.0.0.1:{port}",
            Protocol = Protocol.Tcp,
            HeartbeatInterval = heartbeat ?? Heartbeat,
            ReconnectionSettings = new ReconnectionSettings
            {
                Enabled = reconnect,
                MaxRetries = MaxRetries,
                InitialDelay = TimeSpan.FromMilliseconds(1),
                MaxDelay = TimeSpan.FromMilliseconds(1),
                WaitAfterReconnect = TimeSpan.Zero
            }
        };

    private static ConcurrentQueue<ConnectionState> RecordStates(TcpMessageStream client)
    {
        var states = new ConcurrentQueue<ConnectionState>();
        client.SubscribeConnectionEvents(change =>
        {
            states.Enqueue(change.CurrentState);
            return Task.CompletedTask;
        });
        return states;
    }

    private static ushort UnusedPort()
    {
        using var probe = new MockNode();
        return probe.Port;
    }

    private static async Task<bool> EventuallyAsync(Func<bool> condition)
    {
        var deadline = Environment.TickCount64 + WaitMs;
        while (!condition())
        {
            if (Environment.TickCount64 >= deadline)
            {
                return false;
            }
            await Task.Delay(5, TestContext.Current.CancellationToken);
        }
        return true;
    }

    /// <summary>
    ///     A metadata primary that stays up and a partition primary whose handler throws while it is down, so the
    ///     node closes every connection on its first request. While syncs are held, the metadata primary answers
    ///     no group sync, and nothing after one on the same connection, until the cluster is disposed.
    /// </summary>
    private sealed class LinkCluster : IDisposable
    {
        internal readonly MockNode Coordinator = new();
        internal readonly MockNode Primary = new();
        private readonly TaskCompletionSource _syncRelease = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal volatile bool PrimaryDown;
        internal volatile bool SyncsHeld;
        internal int PrimaryWrites;
        internal Action? OnPrimaryBind;

        internal LinkCluster()
        {
            Coordinator.Serve(request =>
            {
                if (SyncsHeld && request.Code == CommandCodes.SYNC_CONSUMER_GROUP_CODE)
                {
                    _syncRelease.Task.GetAwaiter().GetResult();
                }
                return Handle(request);
            });
            Primary.Serve(request =>
            {
                if (request.Code == BIND_SESSION_CODE)
                {
                    OnPrimaryBind?.Invoke();
                }
                if (PrimaryDown)
                {
                    throw new IOException("outage");
                }
                if (request.Operation == (byte)VsrOperation.StoreConsumerOffset)
                {
                    Interlocked.Increment(ref PrimaryWrites);
                }
                return Handle(request);
            });
        }

        internal MockNode ReplacePrimary()
        {
            var replacement = new MockNode(Primary.Port);
            replacement.Serve(Handle);
            return replacement;
        }

        public void Dispose()
        {
            _syncRelease.TrySetResult();
            Coordinator.Dispose();
            Primary.Dispose();
        }

        private byte[] Handle(MockRequest request)
        {
            if (request.Code == GET_CLUSTER_METADATA_CODE)
            {
                var body = new List<byte>();
                WriteString(body, "links");
                body.AddRange(BitConverter.GetBytes(2u));
                WriteNode(body, Coordinator.Port, true, 0);
                WriteNode(body, Primary.Port, false, 1);
                return Reply(OPERATION_NON_REPLICATED, body.ToArray());
            }
            if (request.Code == CommandCodes.SYNC_CONSUMER_GROUP_CODE)
            {
                // A member that owns no partition yet, so a group poll joins without polling a partition.
                return Reply(OPERATION_NON_REPLICATED, AssignmentBody(1, []));
            }
            return request.Code == CommandCodes.GET_PARTITION_ROUTE_CODE
                ? RouteReply(request, new PartitionContext(17, 0, 1), 1)
                : Answer(request);
        }
    }
}
