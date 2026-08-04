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
import org.apache.iggy.client.async.tcp.LeaderAwareness.LeaderCheck;
import org.apache.iggy.cluster.ClusterMetadata;
import org.apache.iggy.cluster.ClusterNode;
import org.apache.iggy.cluster.ClusterNodeRole;
import org.apache.iggy.cluster.ClusterNodeStatus;
import org.apache.iggy.cluster.TransportEndpoints;
import org.apache.iggy.exception.IggyConnectionException;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

class LeaderAwarenessTest {

    private static ClusterNode node(
            String name, String ip, int tcpPort, ClusterNodeRole role, ClusterNodeStatus status) {
        return new ClusterNode(name, ip, new TransportEndpoints(tcpPort, 8080, 3000, 8070), role, status);
    }

    private static ClusterMetadata cluster(ClusterNode... nodes) {
        return new ClusterMetadata("test-cluster", List.of(nodes));
    }

    private static ClusterMetadata leaderlessCluster() {
        return cluster(
                node("node-0", "iggy-0", 8091, ClusterNodeRole.Follower, ClusterNodeStatus.Healthy),
                node("node-1", "iggy-1", 8092, ClusterNodeRole.Follower, ClusterNodeStatus.Healthy));
    }

    private static ClusterMetadata clusterWithLeaderElsewhere() {
        return cluster(
                node("leader-node", "iggy-leader", 8091, ClusterNodeRole.Leader, ClusterNodeStatus.Healthy),
                node("follower-node", "iggy-follower", 8092, ClusterNodeRole.Follower, ClusterNodeStatus.Healthy));
    }

    @Nested
    class CheckLeader {

        @Test
        void shouldStayForSingleNodeCluster() {
            var metadata =
                    cluster(node("iggy-node", "other-host", 8090, ClusterNodeRole.Leader, ClusterNodeStatus.Healthy));

            var check = LeaderAwareness.checkLeader(metadata, new ConnectionInfo("localhost", 8090));

            assertThat(check).isInstanceOf(LeaderCheck.Stay.class);
        }

        @Test
        void shouldRedirectToHealthyLeaderOnAnotherNode() {
            var check = LeaderAwareness.checkLeader(
                    clusterWithLeaderElsewhere(), new ConnectionInfo("iggy-follower", 8092));

            assertThat(check).isEqualTo(new LeaderCheck.Redirect(new ConnectionInfo("iggy-leader", 8091)));
        }

        @Test
        void shouldStayWhenAlreadyOnLeader() {
            var check =
                    LeaderAwareness.checkLeader(clusterWithLeaderElsewhere(), new ConnectionInfo("iggy-leader", 8091));

            assertThat(check).isInstanceOf(LeaderCheck.Stay.class);
        }

        @Test
        void shouldReportLeaderlessClusterAsNoLeader() {
            var check = LeaderAwareness.checkLeader(leaderlessCluster(), new ConnectionInfo("iggy-1", 8092));

            assertThat(check).isInstanceOf(LeaderCheck.NoLeader.class);
        }

        @Test
        void shouldReportUnhealthyLeaderAsNoLeader() {
            var metadata = cluster(
                    node("leader-node", "iggy-leader", 8091, ClusterNodeRole.Leader, ClusterNodeStatus.Unreachable),
                    node("follower-node", "iggy-follower", 8092, ClusterNodeRole.Follower, ClusterNodeStatus.Healthy));

            var check = LeaderAwareness.checkLeader(metadata, new ConnectionInfo("iggy-follower", 8092));

            assertThat(check).isInstanceOf(LeaderCheck.NoLeader.class);
        }

        @Test
        void shouldStayWhenLeaderTcpTransportIsDisabled() {
            var metadata = cluster(
                    node("leader-node", "iggy-leader", 0, ClusterNodeRole.Leader, ClusterNodeStatus.Healthy),
                    node("follower-node", "iggy-follower", 8092, ClusterNodeRole.Follower, ClusterNodeStatus.Healthy));

            var check = LeaderAwareness.checkLeader(metadata, new ConnectionInfo("iggy-follower", 8092));

            assertThat(check).isInstanceOf(LeaderCheck.Stay.class);
        }

        @Test
        void shouldTreatLocalhostAndLoopbackAsSameAddress() {
            var metadata = cluster(
                    node("leader-node", "127.0.0.1", 8091, ClusterNodeRole.Leader, ClusterNodeStatus.Healthy),
                    node("follower-node", "127.0.0.1", 8092, ClusterNodeRole.Follower, ClusterNodeStatus.Healthy));

            var check = LeaderAwareness.checkLeader(metadata, new ConnectionInfo("localhost", 8091));

            assertThat(check).isInstanceOf(LeaderCheck.Stay.class);
        }

        @Test
        void shouldReportEmptyRosterAsNoLeader() {
            var check = LeaderAwareness.checkLeader(cluster(), new ConnectionInfo("localhost", 8090));

            assertThat(check).isInstanceOf(LeaderCheck.NoLeader.class);
        }
    }

    @Nested
    class AddressComparison {

        private boolean isSameAddress(String host1, int port1, String host2, int port2) {
            return LeaderAwareness.isSameAddress(new ConnectionInfo(host1, port1), new ConnectionInfo(host2, port2));
        }

        @Test
        void shouldMatchIdenticalAddresses() {
            assertThat(isSameAddress("127.0.0.1", 8090, "127.0.0.1", 8090)).isTrue();
        }

        @Test
        void shouldMatchLocalhostAgainstLoopback() {
            assertThat(isSameAddress("localhost", 8090, "127.0.0.1", 8090)).isTrue();
        }

        @Test
        void shouldMatchIpv4LoopbackAgainstIpv6Loopback() {
            assertThat(isSameAddress("127.0.0.1", 8090, "::1", 8090)).isTrue();
        }

        @Test
        void shouldMatchCaseInsensitively() {
            assertThat(isSameAddress("LOCALHOST", 8090, "127.0.0.1", 8090)).isTrue();
            assertThat(isSameAddress("Iggy-Leader", 8091, "iggy-leader", 8091)).isTrue();
        }

        @Test
        void shouldMatchUnspecifiedIpv6AgainstLoopback() {
            assertThat(isSameAddress("[::]", 8090, "[::1]", 8090)).isTrue();
        }

        @Test
        void shouldMatchBracketedIpv6AgainstBareForm() {
            assertThat(isSameAddress("[::1]", 8090, "::1", 8090)).isTrue();
        }

        @Test
        void shouldRejectDifferentPorts() {
            assertThat(isSameAddress("127.0.0.1", 8090, "127.0.0.1", 8091)).isFalse();
        }

        @Test
        void shouldRejectDifferentHosts() {
            assertThat(isSameAddress("192.168.1.1", 8090, "127.0.0.1", 8090)).isFalse();
        }

        @Test
        void shouldNotRewriteHostnamesEmbeddingLocalhost() {
            // .invalid never resolves (RFC 2606), so only mangling the names
            // into each other could make these compare equal
            assertThat(isSameAddress("mylocalhost.invalid", 8090, "my127.0.0.1.invalid", 8090))
                    .isFalse();
        }
    }

    @Nested
    class FindLeaderElsewhere {

        private static final Duration BUDGET = Duration.ofSeconds(2);
        private static final Duration INTERVAL = Duration.ofMillis(10);

        private final ConnectionInfo currentTarget = new ConnectionInfo("iggy-follower", 8092);

        private Optional<ConnectionInfo> findLeader(Supplier<CompletableFuture<ClusterMetadata>> fetch) {
            return LeaderAwareness.findLeaderElsewhere(fetch, currentTarget, BUDGET, INTERVAL)
                    .orTimeout(30, TimeUnit.SECONDS)
                    .join();
        }

        @Test
        void shouldRedirectWithoutPollingWhenLeaderIsKnown() {
            var fetchCount = new AtomicInteger();

            var leader = findLeader(() -> {
                fetchCount.incrementAndGet();
                return CompletableFuture.completedFuture(clusterWithLeaderElsewhere());
            });

            assertThat(leader).contains(new ConnectionInfo("iggy-leader", 8091));
            assertThat(fetchCount).hasValue(1);
        }

        @Test
        void shouldPollThroughLeaderlessElectionUntilLeaderAppears() {
            var fetchCount = new AtomicInteger();

            var leader = findLeader(() -> CompletableFuture.completedFuture(
                    fetchCount.incrementAndGet() < 3 ? leaderlessCluster() : clusterWithLeaderElsewhere()));

            assertThat(leader).contains(new ConnectionInfo("iggy-leader", 8091));
            assertThat(fetchCount).hasValue(3);
        }

        @Test
        void shouldGiveUpOnLeaderlessClusterAfterBudget() {
            var fetchCount = new AtomicInteger();

            var leader = LeaderAwareness.findLeaderElsewhere(
                            () -> {
                                fetchCount.incrementAndGet();
                                return CompletableFuture.completedFuture(leaderlessCluster());
                            },
                            currentTarget,
                            Duration.ofMillis(100),
                            INTERVAL)
                    .orTimeout(30, TimeUnit.SECONDS)
                    .join();

            assertThat(leader).isEmpty();
            assertThat(fetchCount.get()).isGreaterThan(1);
        }

        @Test
        void shouldGiveUpImmediatelyWhenMetadataFetchFails() {
            var fetchCount = new AtomicInteger();

            var leader = findLeader(() -> {
                fetchCount.incrementAndGet();
                return CompletableFuture.failedFuture(new IggyConnectionException("connection lost"));
            });

            assertThat(leader).isEmpty();
            assertThat(fetchCount).hasValue(1);
        }

        @Test
        void shouldGiveUpWhenMetadataFetchThrowsSynchronously() {
            var leader = findLeader(() -> {
                throw new IllegalStateException("no connection");
            });

            assertThat(leader).isEmpty();
        }

        @Test
        void shouldStayWithoutPollingWhenAlreadyOnLeader() {
            var fetchCount = new AtomicInteger();

            var leader = LeaderAwareness.findLeaderElsewhere(
                            () -> {
                                fetchCount.incrementAndGet();
                                return CompletableFuture.completedFuture(clusterWithLeaderElsewhere());
                            },
                            new ConnectionInfo("iggy-leader", 8091),
                            BUDGET,
                            INTERVAL)
                    .orTimeout(30, TimeUnit.SECONDS)
                    .join();

            assertThat(leader).isEmpty();
            assertThat(fetchCount).hasValue(1);
        }
    }

    @Nested
    class RedirectionState {

        @Test
        void shouldCapConsecutiveRedirectsAtThree() {
            var state = new LeaderAwareness.LeaderRedirectionState();
            assertThat(state.canRedirect()).isTrue();

            state.recordRedirect();
            state.recordRedirect();
            assertThat(state.canRedirect()).isTrue();

            state.recordRedirect();
            assertThat(state.canRedirect()).isFalse();
        }

        @Test
        void shouldGrantFreshBudgetToEachCheck() {
            var exhausted = new LeaderAwareness.LeaderRedirectionState();
            exhausted.recordRedirect();
            exhausted.recordRedirect();
            exhausted.recordRedirect();
            assertThat(exhausted.canRedirect()).isFalse();

            var nextCheck = new LeaderAwareness.LeaderRedirectionState();

            assertThat(nextCheck.canRedirect()).isTrue();
        }
    }
}
