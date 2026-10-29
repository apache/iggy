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

package org.apache.iggy.client.blocking.tcp;

import org.apache.iggy.Iggy;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.apache.iggy.client.blocking.IggyBaseClient;
import org.apache.iggy.client.blocking.MessagesClientBaseTest;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.DeferredPollOptions;
import org.apache.iggy.message.FetchedMessages;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PollingStrategy;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.iggy.TestConstants.PERSISTED_TOPIC_OPTIONS;
import static org.apache.iggy.TestConstants.STREAM_NAME;
import static org.apache.iggy.TestConstants.TOPIC_NAME;
import static org.assertj.core.api.Assertions.assertThat;

class MessagesTcpClientTest extends MessagesClientBaseTest {

    private static final Duration SHORT_WAIT = Duration.ofMillis(200);
    private static final Duration LONG_WAIT = Duration.ofSeconds(10);
    // Long enough for a wait to reach the server and park there.
    private static final Duration PARK_DELAY = Duration.ofMillis(300);
    // Far below the long wait, so a wait that a message or a cancel did not end fails it.
    private static final Duration WAKE_BOUND = Duration.ofSeconds(2);
    // The deferred polls and fetches that one client keeps in flight.
    private static final int CLIENT_DEFERRED_SLOTS = 64;
    // Server metrics with one series per shard.
    private static final String PARKED_DEFERRED_REQUESTS = "deferred_poll_pending";
    private static final String CANCELLED_DEFERRED_REQUESTS = "deferred_poll_cancelled_total";
    private static final String METRICS_URL = "http://%s:%d/metrics";
    private static final Duration METRICS_POLL_INTERVAL = Duration.ofMillis(50);
    private static final HttpClient METRICS_CLIENT = HttpClient.newHttpClient();

    @Override
    protected IggyBaseClient getClient() {
        return TcpClientFactory.create(serverHost(), serverTcpPort());
    }

    /*
     * The TCP client resolves balanced and key-based partitioning to an
     * explicit partition id before encoding the frame (the VSR broker routes
     * explicit partitions only), so messages with the same key must land on
     * the same partition.
     */

    @Test
    void shouldCancelAWaitingDeferredPollWithoutCommittingItsPartialBatch() throws InterruptedException {
        setUpStreamAndTopic();
        messagesClient.sendMessages(
                STREAM_NAME, TOPIC_NAME, Partitioning.partitionId(0L), List.of(Message.of("unseen")));
        var asyncClient = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .build();
        try {
            asyncClient.connect().join();
            asyncClient.users().login("iggy", "iggy").join();
            var messages = asyncClient.messages();
            var consumer = Consumer.of(0L);
            messages.pollMessages(
                            STREAM_NAME, TOPIC_NAME, Optional.of(0L), consumer, PollingStrategy.first(), 1L, false)
                    .join();
            Duration wait = Duration.ofMillis(500);
            var pending = messages.pollMessagesDeferred(
                    STREAM_NAME,
                    TOPIC_NAME,
                    Optional.of(0L),
                    consumer,
                    PollingStrategy.next(),
                    2L,
                    true,
                    DeferredPollOptions.defaults().withMaxWait(wait).withMinCount(2));
            Thread.sleep(wait.dividedBy(5).toMillis());
            assertThat(pending.cancel(false)).isTrue();
            Thread.sleep(wait.plusMillis(100).toMillis());

            var polled = messages.pollMessages(
                            STREAM_NAME, TOPIC_NAME, Optional.of(0L), consumer, PollingStrategy.next(), 1L, false)
                    .join();
            assertThat(polled.messages()).hasSize(1);
            assertThat(new String(polled.messages().get(0).payload(), StandardCharsets.UTF_8))
                    .isEqualTo("unseen");
        } finally {
            asyncClient.close().join();
        }
    }

    @Test
    void shouldGroupThePartitionsOfOnePrimaryTogether() {
        setUpStream();
        client.topics().createTopic(STREAM_NAME, TOPIC_NAME.getName(), 2L, PERSISTED_TOPIC_OPTIONS);

        assertThat(messagesClient.readinessGroups(STREAM_NAME, TOPIC_NAME, List.of(1L, 0L)))
                .containsExactly(List.of(1L, 0L));
    }

    @Test
    void shouldSendWhileAProgressPollIsParkedOnThePartition() throws Exception {
        setUpStreamAndTopic();
        var asyncClient = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .build();
        try {
            asyncClient.connect().join();
            asyncClient.users().login("iggy", "iggy").join();
            var messages = asyncClient.messages();
            var parked = messages.pollMessagesDeferred(
                    STREAM_NAME,
                    TOPIC_NAME,
                    Optional.of(0L),
                    Consumer.of(0L),
                    PollingStrategy.next(),
                    1L,
                    true,
                    DeferredPollOptions.defaults().withMaxWait(LONG_WAIT));
            Thread.sleep(PARK_DELAY.toMillis());
            assertThat(parked).isNotDone();

            // A send that waits behind the parked poll of the same client ends only after the long wait.
            messages.sendMessages(STREAM_NAME, TOPIC_NAME, Partitioning.partitionId(0L), List.of(Message.of("m")))
                    .get(WAKE_BOUND.toMillis(), TimeUnit.MILLISECONDS);

            assertThat(parked.get(WAKE_BOUND.toMillis(), TimeUnit.MILLISECONDS).messages())
                    .extracting(message -> new String(message.payload(), StandardCharsets.UTF_8))
                    .containsExactly("m");
        } finally {
            asyncClient.close().join();
        }
    }

    @Test
    void shouldFreeTheClientAndServerSlotsOfCancelledFetches() throws Exception {
        setUpStreamAndTopic();
        String token = accessToken();
        var asyncClient = AsyncIggyTcpClient.builder()
                .host(serverHost())
                .port(serverTcpPort())
                .build();
        try {
            asyncClient.connect().join();
            asyncClient.users().login("iggy", "iggy").join();
            var messages = asyncClient.messages();
            var target = Map.of(0L, PollingStrategy.offset(BigInteger.ZERO));
            List<CompletableFuture<List<FetchedMessages>>> parked = new ArrayList<>();
            for (int wait = 0; wait < CLIENT_DEFERRED_SLOTS; wait++) {
                parked.add(messages.fetchMessages(
                        STREAM_NAME,
                        TOPIC_NAME,
                        target,
                        Consumer.of(0L),
                        1L,
                        false,
                        DeferredPollOptions.defaults().withMaxWait(LONG_WAIT)));
            }
            assertThat(awaitServerMetric(token, PARKED_DEFERRED_REQUESTS, CLIENT_DEFERRED_SLOTS))
                    .as("deferred requests that the server parked")
                    .isGreaterThanOrEqualTo(CLIENT_DEFERRED_SLOTS);
            long cancelledBefore = serverMetric(token, CANCELLED_DEFERRED_REQUESTS);

            assertThat(parked).allSatisfy(wait -> assertThat(wait.cancel(true)).isTrue());

            // The server sends nothing back for a cancelled fetch. It answers a fetch whose time is up without a slot,
            // and the client retries a refusal. So only the server metrics show that the cancel frames freed the
            // server slots.
            long cancelled =
                    awaitServerMetric(token, CANCELLED_DEFERRED_REQUESTS, cancelledBefore + CLIENT_DEFERRED_SLOTS);
            assertThat(cancelled - cancelledBefore)
                    .as("deferred requests that the server cancelled")
                    .isGreaterThanOrEqualTo(CLIENT_DEFERRED_SLOTS);
            // The client holds at most this many deferred requests, so this fetch starts in time only if every cancel
            // freed its client slot.
            assertThat(messages.fetchMessages(
                                    STREAM_NAME,
                                    TOPIC_NAME,
                                    target,
                                    Consumer.of(0L),
                                    1L,
                                    false,
                                    DeferredPollOptions.defaults().withMaxWait(SHORT_WAIT))
                            .get(WAKE_BOUND.toMillis(), TimeUnit.MILLISECONDS))
                    .isEmpty();
        } finally {
            asyncClient.close().join();
        }
    }

    @Test
    void shouldRouteSameMessageKeyToSamePartition() {
        setUpStreamAndTopic();

        var firstResponse = messagesClient.sendMessages(
                STREAM_NAME, TOPIC_NAME, Partitioning.messagesKey("test-key"), List.of(Message.of("first")));
        var secondResponse = messagesClient.sendMessages(
                STREAM_NAME, TOPIC_NAME, Partitioning.messagesKey("test-key"), List.of(Message.of("second")));

        assertThat(firstResponse.confirmations()).hasSize(1);
        assertThat(secondResponse.confirmations()).hasSize(1);
        assertThat(secondResponse.confirmations().get(0).partitionId())
                .isEqualTo(firstResponse.confirmations().get(0).partitionId());
    }

    /*
     * TCP only: the HTTP client does not percent-encode path segments yet, so
     * non-ASCII stream and topic names cannot be addressed over HTTP.
     */
    @Test
    void shouldRoundTripNonAsciiNamesKeyAndPayload() {
        // given
        var streamId = StreamId.of("strumień-世界");
        var topicId = TopicId.of("тема-日本語");
        var stream = client.streams().createStream(streamId.getName());
        trackStream(stream.id());
        client.topics().createTopic(streamId, topicId.getName(), 1L, PERSISTED_TOPIC_OPTIONS);
        String text = "wiadomość 世界 😀";

        // when
        messagesClient.sendMessages(streamId, topicId, Partitioning.messagesKey("klucz-键"), List.of(Message.of(text)));
        var polledMessages = messagesClient.pollMessages(
                streamId, topicId, Optional.of(0L), Consumer.of(0L), PollingStrategy.first(), 10L, false);

        // then
        assertThat(polledMessages.messages()).hasSize(1);
        assertThat(new String(polledMessages.messages().get(0).payload(), StandardCharsets.UTF_8))
                .isEqualTo(text);
    }

    private static String accessToken() throws IOException {
        try (var http = Iggy.httpClientBuilder()
                .blocking()
                .host(serverHost())
                .port(serverHttpPort())
                .build()) {
            return http.users()
                    .login("iggy", "iggy")
                    .accessToken()
                    .orElseThrow()
                    .token();
        }
    }

    private static long awaitServerMetric(String token, String name, long minimum)
            throws IOException, InterruptedException {
        long deadline = System.nanoTime() + WAKE_BOUND.toNanos();
        long sum = serverMetric(token, name);
        while (sum < minimum && System.nanoTime() < deadline) {
            Thread.sleep(METRICS_POLL_INTERVAL.toMillis());
            sum = serverMetric(token, name);
        }
        return sum;
    }

    /** The sum of a server metric over its series. */
    private static long serverMetric(String token, String name) throws IOException, InterruptedException {
        var request = HttpRequest.newBuilder(URI.create(METRICS_URL.formatted(serverHost(), serverHttpPort())))
                .header("Authorization", "Bearer " + token)
                .build();
        var response = METRICS_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as("metrics status").isEqualTo(HttpURLConnection.HTTP_OK);
        return response.body()
                .lines()
                .filter(line -> line.startsWith(name + "{"))
                .mapToLong(line -> Long.parseLong(line.substring(line.lastIndexOf(' ') + 1)))
                .sum();
    }
}
