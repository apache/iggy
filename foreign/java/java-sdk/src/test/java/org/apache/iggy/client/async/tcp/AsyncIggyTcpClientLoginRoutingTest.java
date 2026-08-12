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
import org.apache.iggy.user.IdentityInfo;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AsyncIggyTcpClientLoginRoutingTest {

    private static final IdentityInfo FIRST_IDENTITY = new IdentityInfo(1L, Optional.empty());
    private static final IdentityInfo SECOND_IDENTITY = new IdentityInfo(2L, Optional.empty());

    @Test
    void shouldLoginAfterEveryLeaderRedirect() {
        var client = new TestClient();
        client.enqueueLeader(new ConnectionInfo("leader-a", 8091));
        client.enqueueLeader(new ConnectionInfo("leader-b", 8092));
        client.enqueueStay();
        var attempts = new AtomicInteger();

        var identity = client.loginOnLeader(() -> {
                    client.events.add("login");
                    attempts.incrementAndGet();
                    return CompletableFuture.completedFuture(FIRST_IDENTITY);
                })
                .join();

        assertThat(identity).isEqualTo(FIRST_IDENTITY);
        assertThat(attempts).hasValue(3);
        assertThat(client.retargets)
                .containsExactly(new ConnectionInfo("leader-a", 8091), new ConnectionInfo("leader-b", 8092));
        assertThat(client.events)
                .containsExactly("login", "discover", "retarget", "login", "discover", "retarget", "login", "discover");
    }

    @Test
    void shouldSerializeDiscoveryAndLoginAcrossConcurrentCalls() {
        var client = new TestClient();
        client.enqueueStay();
        client.enqueueStay();
        var firstAttempt = new CompletableFuture<IdentityInfo>();

        var firstLogin = client.loginOnLeader(() -> {
            client.events.add("first-login");
            return firstAttempt;
        });
        var secondLogin = client.loginOnLeader(() -> {
            client.events.add("second-login");
            return CompletableFuture.completedFuture(SECOND_IDENTITY);
        });

        assertThat(client.events).containsExactly("first-login");
        assertThat(secondLogin).isNotDone();

        firstAttempt.complete(FIRST_IDENTITY);

        assertThat(firstLogin.join()).isEqualTo(FIRST_IDENTITY);
        assertThat(secondLogin.join()).isEqualTo(SECOND_IDENTITY);
        assertThat(client.events).containsExactly("first-login", "discover", "second-login", "discover");
    }

    @Test
    void shouldReleaseSerializationGateAfterRoutingFailure() {
        var routingFailure = new IllegalStateException("metadata failed");
        var client = new TestClient();
        client.enqueueDiscovery(CompletableFuture.failedFuture(routingFailure));
        client.enqueueStay();
        var failedAttemptCount = new AtomicInteger();

        var failedLogin = client.loginOnLeader(() -> {
            failedAttemptCount.incrementAndGet();
            return CompletableFuture.completedFuture(FIRST_IDENTITY);
        });
        var nextLogin = client.loginOnLeader(() -> CompletableFuture.completedFuture(SECOND_IDENTITY));

        assertThatThrownBy(failedLogin::join)
                .isInstanceOf(CompletionException.class)
                .hasCause(routingFailure);
        assertThat(failedAttemptCount).hasValue(1);
        assertThat(nextLogin.join()).isEqualTo(SECOND_IDENTITY);
    }

    @Test
    void shouldReleaseSerializationGateAfterLoginFailure() {
        var loginFailure = new IllegalStateException("register failed");
        var client = new TestClient();
        client.enqueueStay();
        client.enqueueStay();

        var failedLogin = client.loginOnLeader(() -> CompletableFuture.failedFuture(loginFailure));
        var nextLogin = client.loginOnLeader(() -> CompletableFuture.completedFuture(SECOND_IDENTITY));

        assertThatThrownBy(failedLogin::join)
                .isInstanceOf(CompletionException.class)
                .hasCause(loginFailure);
        assertThat(nextLogin.join()).isEqualTo(SECOND_IDENTITY);
    }

    @Test
    void shouldKeepGateUntilCancelledCallFinishesItsRegister() {
        var client = new TestClient();
        client.enqueueStay();
        client.enqueueStay();
        var committedRegister = new CompletableFuture<IdentityInfo>();

        var cancelledLogin = client.loginOnLeader(() -> committedRegister);
        cancelledLogin.cancel(false);
        var nextLogin = client.loginOnLeader(() -> CompletableFuture.completedFuture(SECOND_IDENTITY));

        assertThat(nextLogin).isNotDone();

        committedRegister.complete(FIRST_IDENTITY);

        assertThat(nextLogin.join()).isEqualTo(SECOND_IDENTITY);
    }

    private static final class TestClient extends AsyncIggyTcpClient {
        private final Queue<CompletableFuture<Optional<ConnectionInfo>>> discoveries = new ArrayDeque<>();
        private final List<ConnectionInfo> retargets = new ArrayList<>();
        private final List<String> events = new ArrayList<>();

        private TestClient() {
            super("seed", 8090);
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
        CompletableFuture<Optional<ConnectionInfo>> findLeaderElsewhere(ConnectionInfo currentTarget) {
            events.add("discover");
            return discoveries.remove();
        }

        @Override
        CompletableFuture<Void> retarget(ConnectionInfo newTarget) {
            events.add("retarget");
            retargets.add(newTarget);
            return CompletableFuture.completedFuture(null);
        }
    }
}
