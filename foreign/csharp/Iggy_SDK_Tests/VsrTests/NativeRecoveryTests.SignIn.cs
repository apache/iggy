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
using System.Reflection;
using Apache.Iggy.Configuration;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.IggyClient.Implementations;
using Apache.Iggy.Kinds;
using Apache.Iggy.Tests.Utils;
using Apache.Iggy.Utils;
using Apache.Iggy.Vsr;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using static Apache.Iggy.Tests.VsrTests.MockFrames;

namespace Apache.Iggy.Tests.VsrTests;

public sealed partial class NativeRecoveryTests
{
    [Fact]
    public async Task given_session_recovery_when_routing_is_locked_should_not_block_lane_admission()
    {
        const BindingFlags fields = BindingFlags.Instance | BindingFlags.NonPublic;
        var config = new IggyClientConfigurator { BaseAddress = "127.0.0.1:1", Protocol = Protocol.Tcp };
        using var client = new TcpMessageStream(config, NullLoggerFactory.Instance);
        var session = (ConsensusSession)typeof(TcpMessageStream).GetField("_consensusSession", fields)!.GetValue(client)!;
        var routingGate = typeof(TcpMessageStream).GetField("_pollRoutingGate", fields)!.GetValue(client)!;
        var recovery = Signal();
        typeof(TcpMessageStream).GetField("_sessionRecovery", fields)!.SetValue(client, recovery.Task);
        var generation = session.Generation;
        session.Reset(continues: true);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        using var admitted = new ManualResetEventSlim();
        var waiting = Task.CompletedTask;
        try
        {
            lock (routingGate)
            {
                waiting = Task.Run(async () =>
                {
                    var operation = client.Mutations.EnterAsync(MutationLane.Metadata, CommandCodes.CREATE_STREAM_CODE,
                        ReadOnlyMemory<byte>.Empty, default, true, generation, cancellation.Token);
                    admitted.Set();
                    await Assert.ThrowsAnyAsync<OperationCanceledException>(() => operation);
                }, cancellation.Token);
                Assert.True(admitted.Wait(TimeSpan.FromSeconds(5), cancellation.Token),
                    "Lane admission must yield to recovery without taking the routing lock.");
            }
        }
        finally
        {
            await cancellation.CancelAsync();
            recovery.TrySetResult();
            await waiting.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task given_replica_links_when_the_caller_changes_sessions_should_keep_the_logout_transport(bool logout)
    {
        const int sessionChanges = 64;
        using var cluster = new RecoveryCluster();
        using var client = await cluster.ConnectAsync();
        for (var change = 0; change < sessionChanges; change++)
        {
            if (logout)
            {
                await client.LogoutUserAsync(cluster.Token);
                await client.ConnectAsync(cluster.Token);
            }
            await client.LoginUserAsync("caller", "own-secret", cluster.Token);
        }
        await StoreAsync(client, cluster.Token);
        Assert.Single(cluster.Writes);
    }

    [Theory]
    [InlineData(40)]
    [InlineData(42)]
    [InlineData(43)]
    [InlineData(44)]
    [InlineData(47)]
    [InlineData(53)]
    [InlineData(54)]
    [InlineData(4)]
    public async Task given_an_automatic_register_refusal_when_a_call_waits_should_preserve_the_cause_and_require_sign_in(int code)
    {
        using var cluster = new RecoveryCluster();
        var binding = Signal();
        var release = Signal();
        cluster.HoldReply = request => request.Code == CommandCodes.BIND_SESSION_CODE
            ? Hold(binding, release) : Task.CompletedTask;
        using var client = await cluster.ConnectAsync();
        try
        {
            var store = StoreAsync(client, cluster.Token);
            await binding.Task.WaitAsync(cluster.Token);
            cluster.OnRegister = request => Reply(request.Operation, [], (uint)code);
            cluster.PingEviction = EvictionReason.SessionError;
            await Assert.ThrowsAnyAsync<Exception>(() => client.PingAsync(cluster.Token));
            var failure = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => store);
            Assert.Equal(VsrError.UNAUTHENTICATED, failure.StatusCode);
            var cause = Assert.IsType<IggyInvalidStatusCodeException>(failure.InnerException);
            Assert.Equal(code, cause.StatusCode);
            Assert.True(cause.FromServer);
            await client.ConnectAsync(cluster.Token);
            Assert.Equal(2, cluster.Coordinator.Registrations);
            Assert.Empty(cluster.Writes);
        }
        finally
        {
            release.TrySetResult();
        }
    }

    [Theory]
    [InlineData(VsrError.TRANSIENT_NOT_COMMITTED)]
    [InlineData(VsrError.TRANSIENT_NOT_ACCEPTED)]
    [InlineData(VsrError.NOT_PRIMARY)]
    public async Task given_a_transient_register_refusal_when_the_session_recovers_should_sign_in_again(int code)
    {
        // The client resends a register refused with 57 or 58 until its 30 s deadline, before the recovery sees the
        // refusal.
        using var cluster = new RecoveryCluster(TimeSpan.FromSeconds(45));
        using var client = await cluster.ConnectAsync();
        var registrations = new ConcurrentQueue<MockRequest>();
        cluster.OnRegister = request =>
        {
            registrations.Enqueue(request);
            return registrations.TryPeek(out var refused) && refused.ClientId == request.ClientId
                ? Reply(request.Operation, [], (uint)code) : Answer(request);
        };
        cluster.PingEviction = EvictionReason.SessionError;
        await Assert.ThrowsAnyAsync<Exception>(() => client.PingAsync(cluster.Token));
        await cluster.UntilAsync(() => registrations.Select(registration => registration.ClientId).Distinct().Count() == 2);

        await StoreAsync(client, cluster.Token);

        Assert.Equal(registrations.Last().ClientId, Assert.Single(cluster.Writes).ClientId);
    }

    [Fact]
    public async Task given_a_lost_register_reply_when_the_session_recovers_should_sign_in_again()
    {
        using var cluster = new RecoveryCluster();
        using var client = await cluster.ConnectAsync();
        var registrations = new ConcurrentQueue<MockRequest>();
        var lost = 0;
        cluster.OnRegister = request =>
        {
            registrations.Enqueue(request);
            return Answer(request);
        };
        // The lost register may have committed, so the client signs in again under a new client id.
        cluster.BeforeReply = (request, _) =>
        {
            if (request.Operation == (byte)VsrOperation.Register && Interlocked.Exchange(ref lost, 1) == 0)
            {
                throw new IOException("The reply is lost.");
            }
        };
        cluster.PingEviction = EvictionReason.SessionError;
        await Assert.ThrowsAnyAsync<Exception>(() => client.PingAsync(cluster.Token));
        await cluster.UntilAsync(() => registrations.Select(registration => registration.ClientId).Distinct().Count() == 2);

        await StoreAsync(client, cluster.Token);

        Assert.Equal(registrations.Last().ClientId, Assert.Single(cluster.Writes).ClientId);
    }

    [Fact]
    public async Task given_a_held_send_lock_when_a_recovery_register_is_not_admitted_until_its_deadline_should_sign_in_again()
    {
        // The send lock stays held for a second after the register starts, so the 30 s of the call end while the 58
        // resends still run. The register must end with the refusal it holds, so that the recovery signs in again.
        using var cluster = new RecoveryCluster(TimeSpan.FromSeconds(45));
        using var client = await cluster.ConnectAsync();
        var sending = (SemaphoreSlim)typeof(TcpMessageStream)
            .GetField("_sendingSemaphore", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(client)!;
        var authenticating = Signal();
        client.SubscribeConnectionEvents(change =>
        {
            if (change.CurrentState == ConnectionState.Authenticating)
            {
                authenticating.TrySetResult();
            }
            return Task.CompletedTask;
        });
        var registrations = new ConcurrentQueue<MockRequest>();
        cluster.OnRegister = request =>
        {
            registrations.Enqueue(request);
            return registrations.TryPeek(out var refused) && refused.ClientId == request.ClientId
                ? Reply(request.Operation, [], VsrError.TRANSIENT_NOT_ACCEPTED) : Answer(request);
        };
        await sending.WaitAsync(cluster.Token);
        try
        {
            cluster.PingEviction = EvictionReason.SessionError;
            await Assert.ThrowsAnyAsync<Exception>(() => client.PingAsync(cluster.Token));
            await authenticating.Task.WaitAsync(cluster.Token);
            await Task.Delay(TimeSpan.FromSeconds(1), cluster.Token);
        }
        finally
        {
            sending.Release();
        }
        await cluster.UntilAsync(() => registrations.Select(registration => registration.ClientId).Distinct().Count() == 2);

        await StoreAsync(client, cluster.Token);

        Assert.Equal(registrations.Last().ClientId, Assert.Single(cluster.Writes).ClientId);
    }

    [Theory]
    [InlineData("lost link")]
    [InlineData("malformed roster")]
    public async Task given_a_bound_recovery_sign_in_when_its_roster_read_fails_should_keep_the_session(string failure)
    {
        // A lost link ends the roster read only at its 30 s deadline.
        using var cluster = new RecoveryCluster(TimeSpan.FromSeconds(45));
        using var client = await cluster.ConnectAsync();
        const BindingFlags fields = BindingFlags.Instance | BindingFlags.NonPublic;
        var malformed = failure == "malformed roster";
        var corrupt = malformed;
        var lost = 0;
        var registrations = new ConcurrentQueue<MockRequest>();
        cluster.OnRegister = request =>
        {
            registrations.Enqueue(request);
            return Answer(request);
        };
        cluster.BeforeReply = (request, _) =>
        {
            if (!malformed && request.Code == CommandCodes.GET_CLUSTER_METADATA_CODE && !registrations.IsEmpty
                && Interlocked.Exchange(ref lost, 1) == 0)
            {
                throw new IOException("The bootstrap link is lost.");
            }
        };
        cluster.OnRoster = request =>
        {
            if (!Volatile.Read(ref corrupt) || registrations.IsEmpty)
            {
                return null;
            }
            var name = new List<byte>();
            WriteString(name, "recovery");
            return Reply(request.Operation, name.ToArray());
        };
        cluster.PingEviction = EvictionReason.SessionError;
        await Assert.ThrowsAnyAsync<Exception>(() => client.PingAsync(cluster.Token));
        await cluster.UntilAsync(() => !registrations.IsEmpty);
        var binds = Binds(cluster);

        if (malformed)
        {
            // The roster stays malformed, so a recovery that read it again would not end.
            await ((Task)typeof(TcpMessageStream).GetField("_sessionRecovery", fields)!.GetValue(client)!)
                .WaitAsync(cluster.Token);
            Assert.Equal(binds, Binds(cluster));
            Assert.Equal(ConnectionState.Disconnected,
                (ConnectionState)(int)typeof(TcpMessageStream).GetField("_stateValue", fields)!.GetValue(client)!);
            Volatile.Write(ref corrupt, false);
            await client.ConnectAsync(cluster.Token);
        }
        else
        {
            await cluster.UntilAsync(() => Binds(cluster) > binds);
        }
        await StoreAsync(client, cluster.Token);

        Assert.Equal(Assert.Single(registrations).ClientId, Assert.Single(cluster.Writes).ClientId);
    }

    [Theory]
    [InlineData("invalid password")]
    [InlineData("invalid token")]
    [InlineData("refused")]
    [InlineData("evicted")]
    [InlineData("cancelled")]
    [InlineData("timed out")]
    public async Task given_configured_auto_login_when_an_explicit_attempt_fails_should_withdraw_every_fallback(string failure)
    {
        using var cluster = new RecoveryCluster(TimeSpan.FromSeconds(45));
        using var client = ConfiguredClient(cluster);
        await client.ConnectAsync(cluster.Token);
        var register = Signal();
        var release = Signal();
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(cluster.Token);
        if (failure == "refused")
        {
            cluster.OnRegister = request => Reply(request.Operation, [], VsrError.INVALID_CREDENTIALS);
        }
        else if (failure == "evicted")
        {
            cluster.RejectRegister = true;
        }
        else if (failure is "cancelled" or "timed out")
        {
            cluster.HoldReply = request => request.Operation == OPERATION_REGISTER
                ? Hold(register, release) : Task.CompletedTask;
        }
        try
        {
            var attempt = failure == "invalid token"
                ? client.LoginWithPersonalAccessTokenAsync(string.Empty, cancellation.Token)
                : client.LoginUserAsync("caller", failure == "invalid password" ? string.Empty : "own-secret", cancellation.Token);
            if (failure is "cancelled" or "timed out")
            {
                await register.Task.WaitAsync(cluster.Token);
                if (failure == "cancelled")
                {
                    cancellation.Cancel();
                }
            }
            Assert.NotNull(await Record.ExceptionAsync(() => attempt));
            var registrations = cluster.Coordinator.Registrations;
            var binds = Binds(cluster);
            cluster.OnRegister = Answer;
            cluster.RejectRegister = false;
            cluster.HoldReply = null;
            await client.ConnectAsync(cluster.Token);
            Assert.Equal(registrations, cluster.Coordinator.Registrations);
            Assert.Equal(binds, Binds(cluster));
            var refused = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => StoreAsync(client, cluster.Token));
            Assert.Equal(VsrError.UNAUTHENTICATED, refused.StatusCode);
            await client.LoginUserAsync("caller", "own-secret", cluster.Token);
            await StoreAsync(client, cluster.Token);
            Assert.Single(cluster.Writes);
        }
        finally
        {
            release.TrySetResult();
        }
    }

    [Theory]
    [InlineData("refused")]
    [InlineData("closed")]
    [InlineData("cancelled")]
    [InlineData("timed out")]
    public async Task given_a_registered_explicit_attempt_when_roster_discovery_fails_should_discard_its_session(string failure)
    {
        using var cluster = new RecoveryCluster(TimeSpan.FromSeconds(45));
        using var client = await cluster.ConnectAsync();
        var roster = Signal();
        var release = Signal();
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(cluster.Token);
        cluster.OnRoster = request => failure == "refused"
            ? Reply(request.Operation, [], VsrError.UNAUTHORIZED) : null;
        cluster.HoldReply = request => request.Code == CommandCodes.GET_CLUSTER_METADATA_CODE
            && failure is "cancelled" or "timed out" ? Hold(roster, release) : Task.CompletedTask;
        cluster.BeforeReply = (request, _) =>
        {
            if (request.Code == CommandCodes.GET_CLUSTER_METADATA_CODE && failure == "closed")
            {
                throw new IOException("Discovery connection closed.");
            }
        };
        try
        {
            var signIn = client.LoginUserAsync("caller", "own-secret", cancellation.Token);
            if (failure is "cancelled" or "timed out")
            {
                await roster.Task.WaitAsync(cluster.Token);
                if (failure == "cancelled")
                {
                    cancellation.Cancel();
                }
            }
            Assert.NotNull(await Record.ExceptionAsync(() => signIn));
            cluster.OnRoster = null;
            cluster.BeforeReply = null;
            cluster.HoldReply = null;
            var registrations = cluster.Coordinator.Registrations;
            var binds = Binds(cluster);
            await client.ConnectAsync(cluster.Token);
            Assert.Equal(registrations, cluster.Coordinator.Registrations);
            Assert.Equal(binds, Binds(cluster));
            await client.LoginUserAsync("caller", "own-secret", cluster.Token);
            await StoreAsync(client, cluster.Token);
        }
        finally
        {
            release.TrySetResult();
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task given_a_pending_explicit_sign_in_when_a_later_attempt_fails_should_not_restore_the_older_credentials(bool queuedCancellation)
    {
        using var cluster = new RecoveryCluster();
        using var client = await cluster.ConnectAsync();
        var registering = Signal();
        var release = Signal();
        cluster.HoldReply = request => request.Operation == OPERATION_REGISTER
            ? Hold(registering, release) : Task.CompletedTask;
        var earlier = client.LoginUserAsync("earlier", "secret", cluster.Token);
        await registering.Task.WaitAsync(cluster.Token);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(cluster.Token);
        Task later;
        if (queuedCancellation)
        {
            later = client.LoginUserAsync("later", "secret", cancellation.Token);
            cancellation.Cancel();
        }
        else
        {
            later = client.LoginUserAsync("later", string.Empty, cluster.Token);
        }
        release.TrySetResult();
        Assert.NotNull(await Record.ExceptionAsync(() => later));
        Assert.NotNull(await Record.ExceptionAsync(() => earlier));
        cluster.HoldReply = null;
        var registrations = cluster.Coordinator.Registrations;
        await client.ConnectAsync(cluster.Token);
        Assert.Equal(registrations, cluster.Coordinator.Registrations);
        await client.LoginUserAsync("newest", "secret", cluster.Token);
        await StoreAsync(client, cluster.Token);
    }

    [Fact]
    public async Task given_a_held_automatic_register_when_the_caller_signs_in_should_use_the_callers_credentials()
    {
        using var cluster = new RecoveryCluster();
        using var client = await cluster.ConnectAsync();
        var registering = Signal();
        var release = Signal();
        var registrations = new ConcurrentQueue<MockRequest>();
        cluster.OnRegister = request =>
        {
            registrations.Enqueue(request);
            return Reply(request.Operation, [], VsrError.INVALID_CREDENTIALS);
        };
        cluster.HoldReply = request => request.Operation == OPERATION_REGISTER
            ? Hold(registering, release) : Task.CompletedTask;
        cluster.PingEviction = EvictionReason.SessionError;
        await Assert.ThrowsAnyAsync<Exception>(() => client.PingAsync(cluster.Token));
        await registering.Task.WaitAsync(cluster.Token);
        cluster.OnRegister = request =>
        {
            registrations.Enqueue(request);
            return Answer(request);
        };
        cluster.HoldReply = null;
        await client.LoginUserAsync("other", "own-secret", cluster.Token);
        release.TrySetResult();
        await StoreAsync(client, cluster.Token);
        var attempts = registrations.ToArray();
        Assert.Equal(2, attempts.Length);
        Assert.Equal(LoginRegister.Serialize("other", "own-secret", LoginRegister.ReadBindSecret(attempts[1].Body)), attempts[1].Body);
        cluster.PingEviction = null;
        Assert.Equal(attempts[1].ClientId, Assert.Single(cluster.Writes).ClientId);
    }

    [Theory]
    [InlineData("success")]
    [InlineData("reply refusal")]
    [InlineData("eviction refusal")]
    public async Task given_a_committed_password_update_when_an_older_automatic_register_completes_should_keep_the_updated_password(string outcome)
    {
        using var cluster = new RecoveryCluster();
        using var client = await cluster.ConnectAsync();
        var continuations = new HeldContinuations();
        var change = continuations.Run(() => client.ChangePasswordAsync(Identifier.String("user"), "secret", "rotated", cluster.Token));
        await continuations.Posted.WaitAsync(cluster.Token);
        var registering = Signal();
        var release = Signal();
        var attempts = new ConcurrentQueue<MockRequest>();
        cluster.OnRegister = request =>
        {
            attempts.Enqueue(request);
            return attempts.Count == 1 ? outcome switch
            {
                "reply refusal" => Reply(request.Operation, [], VsrError.INVALID_CREDENTIALS),
                "eviction refusal" => Eviction(EvictionReason.InvalidCredentials),
                _ => Answer(request)
            } : Answer(request);
        };
        cluster.HoldReply = request => request.Operation == OPERATION_REGISTER && attempts.Count == 1
            ? Hold(registering, release) : Task.CompletedTask;
        cluster.PingEviction = EvictionReason.SessionError;
        await Assert.ThrowsAnyAsync<Exception>(() => client.PingAsync(cluster.Token));
        await registering.Task.WaitAsync(cluster.Token);
        await continuations.FinishAsync(change, cluster.Token);
        release.TrySetResult();
        await StoreAsync(client, cluster.Token);
        if (outcome == "success")
        {
            var evicted = 0;
            cluster.OnCoordinator = request => request.Code == CommandCodes.PING_CODE && Interlocked.Exchange(ref evicted, 1) == 0
                ? Eviction(EvictionReason.SessionError) : null;
            await Assert.ThrowsAnyAsync<Exception>(() => client.PingAsync(cluster.Token));
            await StoreAsync(client, cluster.Token);
        }
        var registrations = attempts.ToArray();
        Assert.Equal(2, registrations.Length);
        Assert.Equal(LoginRegister.Serialize("user", "secret", LoginRegister.ReadBindSecret(registrations[0].Body)), registrations[0].Body);
        Assert.Equal(LoginRegister.Serialize("user", "rotated", LoginRegister.ReadBindSecret(registrations[1].Body)), registrations[1].Body);
        Assert.Equal(registrations[1].ClientId, cluster.Writes.Last().ClientId);
    }

    [Fact]
    public async Task given_an_old_committed_password_reply_when_another_user_signed_in_should_keep_the_new_users_credentials()
    {
        using var cluster = new RecoveryCluster();
        using var client = await cluster.ConnectAsync();
        var continuations = new HeldContinuations();
        // The mock assigns every registration user id 7, which also exercises identifier reuse across callers.
        var change = continuations.Run(() => client.ChangePasswordAsync(Identifier.Numeric(7), "secret", "rotated", cluster.Token));
        await continuations.Posted.WaitAsync(cluster.Token);
        await client.LoginUserAsync("other", "own-secret", cluster.Token);
        await continuations.FinishAsync(change, cluster.Token);
        var registrations = new ConcurrentQueue<MockRequest>();
        cluster.OnRegister = request =>
        {
            registrations.Enqueue(request);
            return Answer(request);
        };
        cluster.PingEviction = EvictionReason.SessionError;
        await Assert.ThrowsAnyAsync<Exception>(() => client.PingAsync(cluster.Token));
        await StoreAsync(client, cluster.Token);
        var recovered = Assert.Single(registrations);
        Assert.Equal(LoginRegister.Serialize("other", "own-secret", LoginRegister.ReadBindSecret(recovered.Body)), recovered.Body);
    }

    [Fact]
    public async Task given_a_group_poll_waiting_for_sync_when_the_caller_changes_session_should_not_poll_as_the_new_caller()
    {
        using var cluster = new RecoveryCluster();
        using var client = await cluster.ConnectAsync();
        var continuations = new HeldContinuations();
        var poll = continuations.Run(async () =>
        {
            using var messages = await client.PollMessagesRentedAsync(Stream, Topic, null, Consumer.Group(4),
                PollingStrategy.Next(), 1, false, cluster.Token);
        });
        await continuations.Posted.WaitAsync(cluster.Token);
        await client.LoginUserAsync("other", "secret", cluster.Token);
        var failure = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => continuations.FinishAsync(poll, cluster.Token));
        Assert.Equal(VsrError.UNAUTHENTICATED, failure.StatusCode);
        Assert.Empty(cluster.Polls);
    }

    [Fact]
    public async Task given_a_coordinator_eviction_during_another_links_metadata_call_when_redial_fails_should_keep_recovery_running()
    {
        using var cluster = new RecoveryCluster { PrimaryLeads = true };
        var retry = Signal();
        var log = new Moq.Mock<ILogger>();
        log.Setup(logger => logger.IsEnabled(Moq.It.IsAny<LogLevel>())).Returns(true);
        log.Setup(logger => logger.Log(Moq.It.IsAny<LogLevel>(), Moq.It.IsAny<EventId>(),
                Moq.It.IsAny<Moq.It.IsAnyType>(), Moq.It.IsAny<Exception?>(), Moq.It.IsAny<Func<Moq.It.IsAnyType, Exception?, string>>()))
            .Callback(new Moq.InvocationAction(invocation =>
            {
                if (invocation.Arguments[3] is InvalidCertificatePathException)
                {
                    retry.TrySetResult();
                }
            }));
        var loggers = new Moq.Mock<ILoggerFactory>();
        loggers.Setup(factory => factory.CreateLogger(Moq.It.IsAny<string>())).Returns(log.Object);
        var config = new IggyClientConfigurator
        {
            BaseAddress = $"127.0.0.1:{cluster.Coordinator.Port}",
            Protocol = Protocol.Tcp,
            HeartbeatInterval = TimeSpan.FromMilliseconds(25),
            ReconnectionSettings = new ReconnectionSettings
            {
                Enabled = true,
                MaxRetries = 1,
                InitialDelay = TimeSpan.FromMilliseconds(1),
                WaitAfterReconnect = TimeSpan.Zero
            }
        };
        using var client = new TcpMessageStream(config, loggers.Object);
        await client.ConnectAsync(cluster.Token);
        await client.LoginUserAsync("user", "secret", cluster.Token);
        await cluster.UntilAsync(() => Binds(cluster) == 1);
        var metadata = Signal();
        var repaired = false;
        var primaryMetadata = 0;
        var coordinatorMetadata = 0;
        cluster.OnPrimary = request =>
        {
            if (request.Operation == (byte)VsrOperations.ForCode(CommandCodes.CREATE_STREAM_CODE))
            {
                Interlocked.Increment(ref primaryMetadata);
            }
            return null;
        };
        cluster.OnCoordinator = request =>
        {
            if (request.Operation == (byte)VsrOperations.ForCode(CommandCodes.CREATE_STREAM_CODE))
            {
                Interlocked.Increment(ref coordinatorMetadata);
            }
            return null;
        };
        cluster.OnMetadata = request =>
        {
            metadata.TrySetResult();
            return Volatile.Read(ref repaired)
                ? Reply(request.Operation, [.. new byte[4], .. BinaryFactory.CreateStreamPayload(1, 0, "created", 0, 0, 0)])
                : Reply(request.Operation, [], VsrError.TRANSIENT_NOT_ACCEPTED);
        };
        var create = client.CreateStreamAsync("created", token: cluster.Token);
        await metadata.Task.WaitAsync(cluster.Token);
        Assert.Equal(0, Volatile.Read(ref coordinatorMetadata));
        Assert.True(Volatile.Read(ref primaryMetadata) > 0);
        config.TlsSettings.Enabled = true;
        var evicted = 0;
        cluster.OnCoordinator = request => request.Code == CommandCodes.PING_CODE && Interlocked.Exchange(ref evicted, 1) == 0
            ? Eviction(EvictionReason.SessionError) : null;
        await retry.Task.WaitAsync(cluster.Token);
        config.TlsSettings.Enabled = false;
        Volatile.Write(ref repaired, true);
        Assert.NotNull(await create.WaitAsync(cluster.Token));
        Assert.True(cluster.Coordinator.Registrations + cluster.Primary.Registrations >= 2);
    }

    private static TcpMessageStream ConfiguredClient(RecoveryCluster cluster) => new(new IggyClientConfigurator
    {
        BaseAddress = $"127.0.0.1:{cluster.Coordinator.Port}",
        Protocol = Protocol.Tcp,
        HeartbeatInterval = TimeSpan.FromHours(1),
        AutoLoginSettings = AutoLoginSettings.For("configured", "configured-secret"),
        ReconnectionSettings = new ReconnectionSettings
        {
            Enabled = true,
            InitialDelay = TimeSpan.FromMilliseconds(1),
            WaitAfterReconnect = TimeSpan.Zero
        }
    }, NullLoggerFactory.Instance);

    private static TaskCompletionSource Signal() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    private static Task Hold(TaskCompletionSource started, TaskCompletionSource release)
    {
        started.TrySetResult();
        return release.Task;
    }

    private sealed class HeldContinuations : SynchronizationContext
    {
        private readonly ConcurrentQueue<(SendOrPostCallback Callback, object? State)> _pending = new();
        internal SemaphoreSlim Posted { get; } = new(0);

        public override void Post(SendOrPostCallback callback, object? state)
        {
            _pending.Enqueue((callback, state));
            Posted.Release();
        }

        internal Task Run(Func<Task> action)
        {
            var previous = Current;
            SetSynchronizationContext(this);
            try
            {
                return action();
            }
            finally
            {
                SetSynchronizationContext(previous);
            }
        }

        internal async Task FinishAsync(Task task, CancellationToken token)
        {
            while (!task.IsCompleted)
            {
                if (_pending.TryDequeue(out var continuation))
                {
                    _ = Run(() =>
                    {
                        continuation.Callback(continuation.State);
                        return Task.CompletedTask;
                    });
                }
                else
                {
                    await Posted.WaitAsync(token);
                }
            }
            await task;
        }
    }
}
