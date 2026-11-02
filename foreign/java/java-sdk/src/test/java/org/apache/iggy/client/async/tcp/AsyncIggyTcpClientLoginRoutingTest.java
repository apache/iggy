/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iggy.client.async.tcp;

import org.apache.iggy.client.ConnectionInfo;
import org.apache.iggy.client.async.tcp.vsr.ConsensusSession;
import org.apache.iggy.client.async.tcp.vsr.VsrFrameDecoder;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.user.IdentityInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AsyncIggyTcpClientLoginRoutingTest {

    private static final IdentityInfo FIRST_IDENTITY = new IdentityInfo(1L, Optional.empty());
    private static final IdentityInfo SECOND_IDENTITY = new IdentityInfo(2L, Optional.empty());

    private final List<TestClient> clients = new ArrayList<>();

    @AfterEach
    void closeClients() throws Exception {
        for (var client : clients) {
            client.close().get(5, TimeUnit.SECONDS);
        }
    }

    private TestClient client(Duration timeout) throws ReflectiveOperationException {
        var client = new TestClient(timeout);
        clients.add(client);
        return client;
    }

    @Test
    void shouldKeepTheRegisteredIdentityWhenBindingTheMetadataLeader() throws Exception {
        var client = client(null);
        client.enqueueLeader(new ConnectionInfo("leader-a", 8091));
        client.enqueueLeader(new ConnectionInfo("leader-b", 8092));
        client.enqueueStay();
        var attempts = new AtomicInteger();

        var identity = client.loginOnLeader(deadline -> {
                    client.events.add("login");
                    attempts.incrementAndGet();
                    return CompletableFuture.completedFuture(FIRST_IDENTITY);
                })
                .get(5, TimeUnit.SECONDS);

        assertThat(identity).isEqualTo(FIRST_IDENTITY);
        assertThat(attempts).hasValue(1);
        assertThat(client.retargets).containsExactly(new ConnectionInfo("leader-a", 8091));
        assertThat(client.events).containsExactly("login", "discover", "retarget");
    }

    @Test
    void shouldSerializeDiscoveryAndLoginAcrossConcurrentCalls() throws Exception {
        var client = client(null);
        client.enqueueStay();
        client.enqueueStay();
        var firstAttempt = new CompletableFuture<IdentityInfo>();

        var firstLogin = client.loginOnLeader(deadline -> {
            client.events.add("first-login");
            return firstAttempt;
        });
        var secondLogin = client.loginOnLeader(deadline -> {
            client.events.add("second-login");
            return CompletableFuture.completedFuture(SECOND_IDENTITY);
        });

        assertThat(client.events).containsExactly("first-login");
        assertThat(secondLogin).isNotDone();

        firstAttempt.complete(FIRST_IDENTITY);

        assertThat(firstLogin.get(5, TimeUnit.SECONDS)).isEqualTo(FIRST_IDENTITY);
        assertThat(secondLogin.get(5, TimeUnit.SECONDS)).isEqualTo(SECOND_IDENTITY);
        assertThat(client.events).containsExactly("first-login", "second-login", "discover");
    }

    @Test
    void shouldReleaseSerializationGateAfterRoutingFailure() throws Exception {
        var routingFailure = new IllegalStateException("metadata failed");
        var client = client(null);
        client.enqueueDiscovery(CompletableFuture.failedFuture(routingFailure));
        client.enqueueStay();
        var failedAttemptCount = new AtomicInteger();

        var failedLogin = client.loginOnLeader(deadline -> {
            failedAttemptCount.incrementAndGet();
            return CompletableFuture.completedFuture(FIRST_IDENTITY);
        });
        var nextLogin = client.loginOnLeader(deadline -> CompletableFuture.completedFuture(SECOND_IDENTITY));

        assertThatThrownBy(() -> failedLogin.get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCause(routingFailure);
        assertThat(failedAttemptCount).hasValue(1);
        assertThat(nextLogin.get(5, TimeUnit.SECONDS)).isEqualTo(SECOND_IDENTITY);
    }

    @Test
    void shouldReleaseSerializationGateAfterLoginFailure() throws Exception {
        var loginFailure = new IllegalStateException("register failed");
        var client = client(null);
        client.enqueueStay();
        client.enqueueStay();

        var failedLogin = client.loginOnLeader(deadline -> CompletableFuture.failedFuture(loginFailure));
        var nextLogin = client.loginOnLeader(deadline -> CompletableFuture.completedFuture(SECOND_IDENTITY));

        assertThatThrownBy(() -> failedLogin.get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCause(loginFailure);
        assertThat(nextLogin.get(5, TimeUnit.SECONDS)).isEqualTo(SECOND_IDENTITY);
    }

    @Test
    void shouldKeepGateUntilCancelledCallFinishesItsRegister() throws Exception {
        var client = client(null);
        client.enqueueStay();
        client.enqueueStay();
        var committedRegister = new CompletableFuture<IdentityInfo>();

        var cancelledLogin = client.loginOnLeader(deadline -> committedRegister);
        cancelledLogin.cancel(false);
        var nextLogin = client.loginOnLeader(deadline -> CompletableFuture.completedFuture(SECOND_IDENTITY));

        assertThat(nextLogin).isNotDone();

        committedRegister.complete(FIRST_IDENTITY);

        assertThat(nextLogin.get(5, TimeUnit.SECONDS)).isEqualTo(SECOND_IDENTITY);
    }

    @Test
    void shouldExpireQueuedLoginWithoutLettingItsSuccessorOvertakeTheActiveRegister() throws Exception {
        var client = client(Duration.ofMillis(100));
        client.enqueueStay();
        client.enqueueStay();
        var activeRegister = new CompletableFuture<IdentityInfo>();
        var first = client.loginOnLeader(deadline -> activeRegister);
        AtomicInteger queuedAttempts = new AtomicInteger();
        var expired = client.loginOnLeader(deadline -> {
            queuedAttempts.incrementAndGet();
            return CompletableFuture.completedFuture(SECOND_IDENTITY);
        });
        AsyncIggyTcpClientTransientFailoverTest.assertServerError(
                expired, IggyErrorCode.TRANSIENT_NOT_ACCEPTED.getCode());
        var successor = client.loginOnLeader(deadline -> {
            queuedAttempts.incrementAndGet();
            return CompletableFuture.completedFuture(SECOND_IDENTITY);
        });
        assertThat(successor).isNotDone();
        assertThat(queuedAttempts).hasValue(0);
        activeRegister.complete(FIRST_IDENTITY);
        assertThat(first.get(5, TimeUnit.SECONDS)).isEqualTo(FIRST_IDENTITY);
        assertThat(successor.get(5, TimeUnit.SECONDS)).isEqualTo(SECOND_IDENTITY);
        assertThat(queuedAttempts).hasValue(1);
    }

    private static final class TestClient extends AsyncIggyTcpClient {
        private final Queue<CompletableFuture<Optional<ConnectionInfo>>> discoveries = new ArrayDeque<>();
        private final List<ConnectionInfo> retargets = new ArrayList<>();
        private final List<String> events = new ArrayList<>();

        @SuppressWarnings("unchecked")
        private TestClient(Duration timeout) throws ReflectiveOperationException {
            super(
                    "seed",
                    8090,
                    null,
                    null,
                    null,
                    null,
                    timeout,
                    Duration.ofSeconds(5),
                    VsrFrameDecoder.DEFAULT_MAX_FRAME_SIZE,
                    null,
                    false,
                    Optional.empty(),
                    Optional.empty(),
                    AsyncTcpConnection.DEFAULT_IO_THREADS);
            // Successful login publication retains the carrier's credentials even when this fixture fakes Register.
            var carrier = new AsyncTcpConnection(
                    "localhost",
                    8090,
                    false,
                    Optional.empty(),
                    AsyncTcpConnection.TcpConnectionPoolConfig.builder().build(),
                    Optional.empty());
            var sessionField = AsyncIggyTcpClient.class.getDeclaredField("session");
            sessionField.setAccessible(true);
            carrier.followSession((ConsensusSession) sessionField.get(this));
            var connectionField = AsyncIggyTcpClient.class.getDeclaredField("connection");
            connectionField.setAccessible(true);
            ((AtomicReference<AsyncTcpConnection>) connectionField.get(this)).set(carrier);
        }

        private void enqueueLeader(ConnectionInfo target) {
            enqueueDiscovery(CompletableFuture.completedFuture(Optional.of(target)));
        }

        private void enqueueStay() {
            enqueueDiscovery(CompletableFuture.completedFuture(Optional.empty()));
        }

        private void enqueueDiscovery(CompletableFuture<Optional<ConnectionInfo>> discovery) {
            discoveries.add(discovery);
        }

        @Override
        CompletableFuture<Optional<ConnectionInfo>> findLeaderElsewhere(ConnectionInfo currentTarget, long deadline) {
            events.add("discover");
            return discoveries.remove();
        }

        @Override
        CompletableFuture<Void> retarget(ConnectionInfo newTarget, long deadline, BooleanSupplier wanted) {
            events.add("retarget");
            retargets.add(newTarget);
            return CompletableFuture.completedFuture(null);
        }
    }
}
