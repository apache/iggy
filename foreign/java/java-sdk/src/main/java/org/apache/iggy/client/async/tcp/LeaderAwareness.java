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
import org.apache.iggy.cluster.ClusterMetadata;
import org.apache.iggy.cluster.ClusterNode;
import org.apache.iggy.cluster.ClusterNodeRole;
import org.apache.iggy.cluster.ClusterNodeStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Leader selection logic for cluster-aware TCP clients.
 */
final class LeaderAwareness {

    static final int MAX_LEADER_REDIRECTS = 3;

    /**
     * How long to wait for a transiently leaderless cluster to elect before
     * proceeding on the current node anyway. A restarted node cedes the
     * primaryship its stale view assigns it, and until the peers' election
     * completes the roster reports no leader; that window is roughly one
     * heartbeat timeout.
     */
    static final Duration LEADERLESS_WAIT_BUDGET = Duration.ofSeconds(5);

    static final Duration LEADERLESS_POLL_INTERVAL = Duration.ofMillis(250);

    private static final Logger log = LoggerFactory.getLogger(LeaderAwareness.class);

    private LeaderAwareness() {}

    /**
     * Finds a healthy leader living elsewhere, polling the roster through a
     * transiently leaderless cluster for up to {@link #LEADERLESS_WAIT_BUDGET}.
     * Resolves to empty when the client should stay on the current node: it
     * already is the leader, the cluster is single-node, no leader appeared
     * within the budget, or the roster could not be fetched. Never resolves
     * exceptionally, so the redirection path cannot fail the login that
     * triggered it.
     */
    static CompletableFuture<Optional<ConnectionInfo>> findLeaderElsewhere(
            Supplier<CompletableFuture<ClusterMetadata>> fetchMetadata, ConnectionInfo currentTarget) {
        return findLeaderElsewhere(fetchMetadata, currentTarget, LEADERLESS_WAIT_BUDGET, LEADERLESS_POLL_INTERVAL);
    }

    static CompletableFuture<Optional<ConnectionInfo>> findLeaderElsewhere(
            Supplier<CompletableFuture<ClusterMetadata>> fetchMetadata,
            ConnectionInfo currentTarget,
            Duration leaderlessWaitBudget,
            Duration leaderlessPollInterval) {
        long electionDeadlineNanos = System.nanoTime() + leaderlessWaitBudget.toNanos();
        return pollForLeader(
                fetchMetadata, currentTarget, leaderlessWaitBudget, leaderlessPollInterval, electionDeadlineNanos);
    }

    private static CompletableFuture<Optional<ConnectionInfo>> pollForLeader(
            Supplier<CompletableFuture<ClusterMetadata>> fetchMetadata,
            ConnectionInfo currentTarget,
            Duration leaderlessWaitBudget,
            Duration leaderlessPollInterval,
            long electionDeadlineNanos) {
        CompletableFuture<ClusterMetadata> fetched;
        try {
            fetched = fetchMetadata.get();
        } catch (RuntimeException fetchError) {
            fetched = CompletableFuture.failedFuture(fetchError);
        }
        return fetched.<CompletableFuture<Optional<ConnectionInfo>>>handleAsync((metadata, error) -> {
                    if (error != null) {
                        log.warn(
                                "Failed to get cluster metadata: {}, connection will continue on server node {}",
                                error.getMessage(),
                                currentTarget.serverAddress());
                        return CompletableFuture.completedFuture(Optional.empty());
                    }
                    LeaderCheck check;
                    try {
                        check = checkLeader(metadata, currentTarget);
                    } catch (RuntimeException selectionError) {
                        log.warn(
                                "Failed to select leader from cluster metadata: {}, connection will continue"
                                        + " on server node {}",
                                selectionError.getMessage(),
                                currentTarget.serverAddress());
                        return CompletableFuture.completedFuture(Optional.empty());
                    }
                    if (check instanceof LeaderCheck.Redirect redirect) {
                        return CompletableFuture.completedFuture(Optional.of(redirect.target()));
                    }
                    if (check instanceof LeaderCheck.NoLeader) {
                        if (System.nanoTime() >= electionDeadlineNanos) {
                            log.warn(
                                    "No active leader found in cluster metadata within {}, connection will"
                                            + " continue on server node {}",
                                    leaderlessWaitBudget,
                                    currentTarget.serverAddress());
                            return CompletableFuture.completedFuture(Optional.empty());
                        }
                        Executor retryAfterInterval = CompletableFuture.delayedExecutor(
                                leaderlessPollInterval.toMillis(), TimeUnit.MILLISECONDS);
                        return CompletableFuture.supplyAsync(
                                        () -> pollForLeader(
                                                fetchMetadata,
                                                currentTarget,
                                                leaderlessWaitBudget,
                                                leaderlessPollInterval,
                                                electionDeadlineNanos),
                                        retryAfterInterval)
                                .thenCompose(Function.identity());
                    }
                    return CompletableFuture.completedFuture(Optional.empty());
                })
                .thenCompose(Function.identity());
    }

    /**
     * One leader-check verdict from a cluster-metadata snapshot.
     */
    static LeaderCheck checkLeader(ClusterMetadata metadata, ConnectionInfo currentTarget) {
        var nodes = metadata.nodes();
        if (nodes.size() == 1) {
            log.debug(
                    "Single-node cluster detected ({}), no leader redirection needed",
                    nodes.get(0).name());
            return new LeaderCheck.Stay();
        }

        ClusterNode leader = nodes.stream()
                .filter(node -> node.role() == ClusterNodeRole.Leader && node.status() == ClusterNodeStatus.Healthy)
                .findFirst()
                .orElse(null);
        if (leader == null) {
            log.debug(
                    "No active leader found in cluster metadata for current target {}", currentTarget.serverAddress());
            return new LeaderCheck.NoLeader();
        }

        var leaderTcpPort = leader.endpoints().tcp();
        if (leaderTcpPort == 0) {
            log.warn(
                    "Leader node {} has the tcp transport disabled, connection will continue on server node {}",
                    leader.name(),
                    currentTarget.serverAddress());
            return new LeaderCheck.Stay();
        }

        var leaderTarget = new ConnectionInfo(leader.ip(), leaderTcpPort);
        if (isSameAddress(currentTarget, leaderTarget)) {
            log.debug("Already connected to leader at {}", currentTarget.serverAddress());
            return new LeaderCheck.Stay();
        }

        log.info(
                "Found leader node {} at {}, current connection to {} is not the leader",
                leader.name(),
                leaderTarget.serverAddress(),
                currentTarget.serverAddress());
        return new LeaderCheck.Redirect(leaderTarget);
    }

    /**
     * Whether two targets name the same endpoint. Ports must match; hosts
     * match when they are textually equal after canonicalization, when both
     * only reach the local machine, or when they resolve to a common
     * address. A hostname the resolver does not know compares unequal, which
     * at worst costs one redirect hop back to the same node.
     */
    static boolean isSameAddress(ConnectionInfo target1, ConnectionInfo target2) {
        if (target1.port() != target2.port()) {
            return false;
        }
        var host1 = canonicalHost(target1.host());
        var host2 = canonicalHost(target2.host());
        if (host1.equals(host2)) {
            return true;
        }
        return resolveToSameHost(host1, host2);
    }

    private static String canonicalHost(String host) {
        var canonical = host.toLowerCase(Locale.ROOT);
        if (canonical.startsWith("[") && canonical.endsWith("]")) {
            canonical = canonical.substring(1, canonical.length() - 1);
        }
        return canonical;
    }

    private static boolean resolveToSameHost(String host1, String host2) {
        InetAddress[] addresses1;
        InetAddress[] addresses2;
        try {
            addresses1 = InetAddress.getAllByName(host1);
            addresses2 = InetAddress.getAllByName(host2);
        } catch (UnknownHostException resolutionError) {
            log.debug("Could not compare hosts {} and {} by address: {}", host1, host2, resolutionError.getMessage());
            return false;
        }
        if (reachesOnlyLocalMachine(addresses1) && reachesOnlyLocalMachine(addresses2)) {
            return true;
        }
        List<InetAddress> candidates = Arrays.asList(addresses2);
        return Arrays.stream(addresses1).anyMatch(candidates::contains);
    }

    /**
     * Loopback and unspecified addresses all reach the local machine, so
     * localhost, 127.0.0.1, ::1 and [::] name the same endpoint on a given
     * port.
     */
    private static boolean reachesOnlyLocalMachine(InetAddress[] addresses) {
        return Arrays.stream(addresses).allMatch(address -> address.isLoopbackAddress() || address.isAnyLocalAddress());
    }

    /**
     * One leader-check verdict from a cluster-metadata snapshot.
     */
    sealed interface LeaderCheck {

        /** A healthy leader with an enabled tcp transport lives elsewhere; reconnect to it. */
        record Redirect(ConnectionInfo target) implements LeaderCheck {}

        /** Stay on the current node: it is the leader, the cluster is single-node, or the leader is unusable. */
        record Stay() implements LeaderCheck {}

        /** No healthy leader is marked, e.g. mid-election; worth re-polling. */
        record NoLeader() implements LeaderCheck {}
    }

    /**
     * Counts the redirection hops of a single leader check so two nodes that
     * both claim leadership cannot ping-pong a client forever. Failed
     * reconnect attempts do not count. The state lives for one check only;
     * the next login starts with a fresh budget, so exhausting the cap never
     * parks the client on a follower for its whole lifetime.
     */
    static final class LeaderRedirectionState {

        private final AtomicInteger redirectCount = new AtomicInteger();

        boolean canRedirect() {
            return redirectCount.get() < MAX_LEADER_REDIRECTS;
        }

        void recordRedirect() {
            redirectCount.incrementAndGet();
        }
    }
}
