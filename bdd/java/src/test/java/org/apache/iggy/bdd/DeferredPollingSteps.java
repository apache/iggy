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

package org.apache.iggy.bdd;

import io.cucumber.java.After;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.DeferredPollOptions;
import org.apache.iggy.message.FetchedMessages;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class DeferredPollingSteps {

    // The Java SDK has no default consumer, so this is the default consumer id of the Rust SDK.
    private static final Consumer DEFAULT_CONSUMER = Consumer.of(1L);
    private static final long POLL_COUNT = 10;
    private static final Duration ASSIGNMENT_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration ASSIGNMENT_POLL_INTERVAL = Duration.ofMillis(10);

    private AsyncIggyTcpClient client;
    private Long streamId;
    private String topicName;
    private long partitionsCount;
    private ConsumerId groupId;
    private CompletableFuture<PolledMessages> poll;
    private long pollStartedAtNanos;
    private List<CompletableFuture<List<FetchedMessages>>> fetches = List.of();

    // A cancel sends the cancel frame of a call that a failed scenario left parked.
    @After
    public void close() {
        if (client == null) {
            return;
        }
        if (poll != null) {
            poll.cancel(true);
        }
        fetches.forEach(fetch -> fetch.cancel(true));
        try {
            if (streamId != null) {
                client.streams().deleteStream(StreamId.of(streamId)).join();
            }
        } finally {
            client.close().join();
        }
    }

    @Given("a stream named {string} with a topic named {string} with {int} partitions exists")
    public void streamWithTopic(String streamName, String topic, int partitions) {
        client().streams()
                .getStream(StreamId.of(streamName))
                .join()
                .ifPresent(existing -> client().streams()
                        .deleteStream(StreamId.of(existing.id()))
                        .join());
        streamId = client().streams().createStream(streamName).join().id();
        client().topics().createTopic(stream(), topic, partitions).join();
        topicName = topic;
        partitionsCount = partitions;
    }

    @When("I start a deferred poll of partition {int} from offset {int} with a max wait of {int} second(s)")
    public void startOffsetPoll(int partition, int offset, int maxWaitSeconds) {
        startPoll(partition, DEFAULT_CONSUMER, offset(offset), false, options(maxWaitSeconds));
    }

    @When(
            "I start a deferred poll of partition {int} for consumer {string} with auto commit, a min count of {int} and a max wait of {int} second(s)")
    public void startProgressPoll(int partition, String consumer, int minCount, int maxWaitSeconds) {
        startPoll(
                partition,
                Consumer.of(ConsumerId.of(consumer)),
                PollingStrategy.next(),
                true,
                options(maxWaitSeconds).withMinCount(minCount));
    }

    @When("I send a message with payload {string} to partition {int}")
    public void sendToPartition(String payload, int partition) {
        client().messages()
                .sendMessages(
                        stream(), topic(), Partitioning.partitionId((long) partition), List.of(Message.of(payload)))
                .join();
    }

    @Then("the deferred poll should return the payload {string} within {int} seconds")
    public void pollReturnsPayload(String payload, int seconds) throws InterruptedException, ExecutionException {
        assertEquals(List.of(payload), payloads(within(poll, Duration.ofSeconds(seconds))));
    }

    @Then("the deferred poll should return no messages between {int} and {int} seconds after it started")
    public void pollReturnsNothing(int fromSeconds, int toSeconds) throws InterruptedException, ExecutionException {
        Duration bound = Duration.ofSeconds(toSeconds).minusNanos(System.nanoTime() - pollStartedAtNanos);
        PolledMessages polled = within(poll, bound);
        Duration elapsed = Duration.ofNanos(System.nanoTime() - pollStartedAtNanos);

        assertTrue(polled.messages().isEmpty(), polled.toString());
        assertTrue(elapsed.compareTo(Duration.ofSeconds(fromSeconds)) >= 0, "The poll ended after " + elapsed);
    }

    @When("I start a fetch on partitions {int}, {int} and {int} from offset {int} with a max wait of {int} second(s)")
    public void startFetch(int first, int second, int third, int offset, int maxWaitSeconds) {
        fetches = fetchGroups(
                List.of((long) first, (long) second, (long) third),
                offset(offset),
                DEFAULT_CONSUMER,
                false,
                options(maxWaitSeconds));
    }

    @Then("the fetch should return only partition {int} within {int} second(s)")
    public void fetchReturns(int partition, int seconds) throws InterruptedException, ExecutionException {
        List<FetchedMessages> sections = fetchedWithin(fetches, Duration.ofSeconds(seconds));
        assertEquals(
                List.of((long) partition),
                sections.stream().map(FetchedMessages::partitionId).toList());
        assertFalse(sections.get(0).decode().messages().isEmpty(), "The fetch returned no records");
    }

    // The server installs the ownership of the partitions after the join returns.
    @Given("I am the only member of consumer group {string} on the topic")
    public void onlyGroupMember(String name) throws InterruptedException {
        var groups = client().consumerGroups();
        groupId = ConsumerId.of(
                groups.createConsumerGroup(stream(), topic(), name).join().id());
        groups.joinConsumerGroup(stream(), topic(), groupId).join();
        long deadline = System.nanoTime() + ASSIGNMENT_TIMEOUT.toNanos();
        int owned = 0;
        while (owned < partitionsCount && System.nanoTime() < deadline) {
            Thread.sleep(ASSIGNMENT_POLL_INTERVAL.toMillis());
            owned = groups.syncConsumerGroup(stream(), topic(), groupId)
                    .join()
                    .map(assignment -> assignment.partitions().size())
                    .orElse(0);
        }
        assertEquals(partitionsCount, owned, "partitions that the member owns");
    }

    @Then("a group fetch with a max wait of {int} second(s) should return the payload {string} within {int} second(s)")
    public void groupFetchReturnsPayload(int maxWaitSeconds, String payload, int seconds)
            throws InterruptedException, ExecutionException {
        List<Long> partitions = client().consumerGroups()
                .syncConsumerGroup(stream(), topic(), groupId)
                .join()
                .orElseThrow()
                .partitions();
        var groupFetches =
                fetchGroups(partitions, PollingStrategy.next(), Consumer.group(groupId), true, options(maxWaitSeconds));
        List<String> received = fetchedWithin(groupFetches, Duration.ofSeconds(seconds)).stream()
                .flatMap(section -> payloads(section.decode()).stream())
                .toList();
        assertEquals(List.of(payload), received);
    }

    @When("I cancel the deferred poll after {int} milliseconds")
    public void cancelPoll(int millis) throws InterruptedException {
        Thread.sleep(millis);

        assertTrue(poll.cancel(true), "The poll ended before the cancel");
        assertTrue(poll.isCancelled());
    }

    @When("I wait {int} seconds")
    public void waitSeconds(int seconds) throws InterruptedException {
        Thread.sleep(Duration.ofSeconds(seconds).toMillis());
    }

    @Then("a poll of partition {int} for consumer {string} with the next strategy should return the payload {string}")
    public void immediatePollReturnsPayload(int partition, String consumer, String payload) {
        var polled = client().messages()
                .pollMessages(
                        stream(),
                        topic(),
                        Optional.of((long) partition),
                        Consumer.of(ConsumerId.of(consumer)),
                        PollingStrategy.next(),
                        POLL_COUNT,
                        false)
                .join();

        assertFalse(polled.messages().isEmpty(), "The poll read no message");
        assertEquals(payload, payloads(polled).get(0));
    }

    private void startPoll(
            int partition,
            Consumer consumer,
            PollingStrategy strategy,
            boolean autoCommit,
            DeferredPollOptions options) {
        pollStartedAtNanos = System.nanoTime();
        poll = client().messages()
                .pollMessagesDeferred(
                        stream(),
                        topic(),
                        Optional.of((long) partition),
                        consumer,
                        strategy,
                        POLL_COUNT,
                        autoCommit,
                        options);
    }

    private List<CompletableFuture<List<FetchedMessages>>> fetchGroups(
            List<Long> partitions,
            PollingStrategy strategy,
            Consumer consumer,
            boolean autoCommit,
            DeferredPollOptions options) {
        return client().messages().readinessGroups(stream(), topic(), partitions).join().stream()
                .map(group -> {
                    Map<Long, PollingStrategy> targets = new LinkedHashMap<>();
                    group.forEach(partition -> targets.put(partition, strategy));
                    return client().messages()
                            .fetchMessages(stream(), topic(), targets, consumer, POLL_COUNT, autoCommit, options);
                })
                .toList();
    }

    private static List<FetchedMessages> fetchedWithin(
            List<CompletableFuture<List<FetchedMessages>>> calls, Duration bound)
            throws InterruptedException, ExecutionException {
        within(CompletableFuture.allOf(calls.toArray(CompletableFuture[]::new)), bound);
        return calls.stream().flatMap(call -> call.join().stream()).toList();
    }

    private AsyncIggyTcpClient client() {
        if (client == null) {
            String[] address = TestEnvironment.serverAddress().split(":", 2);
            client = AsyncIggyTcpClient.builder()
                    .host(address[0])
                    .port(Integer.parseInt(address[1]))
                    .build();
            client.connect().join();
            client.users()
                    .login(TestEnvironment.rootUsername(), TestEnvironment.rootPassword())
                    .join();
        }
        return client;
    }

    private StreamId stream() {
        return StreamId.of(streamId);
    }

    private TopicId topic() {
        return TopicId.of(topicName);
    }

    // The options refuse a request timeout shorter than the max wait plus the reply margin.
    private static DeferredPollOptions options(int maxWaitSeconds) {
        Duration maxWait = Duration.ofSeconds(maxWaitSeconds);
        Duration timeout = maxWait.plus(DeferredPollOptions.REPLY_MARGIN);
        return DeferredPollOptions.defaults()
                .withRequestTimeout(
                        timeout.compareTo(DeferredPollOptions.DEFAULT_REQUEST_TIMEOUT) > 0
                                ? timeout
                                : DeferredPollOptions.DEFAULT_REQUEST_TIMEOUT)
                .withMaxWait(maxWait);
    }

    private static PollingStrategy offset(int offset) {
        return PollingStrategy.offset(BigInteger.valueOf(offset));
    }

    private static <T> T within(CompletableFuture<T> call, Duration bound)
            throws InterruptedException, ExecutionException {
        try {
            return call.get(Math.max(0, bound.toNanos()), TimeUnit.NANOSECONDS);
        } catch (TimeoutException timeout) {
            throw new AssertionError("The call did not end within " + bound, timeout);
        }
    }

    private static List<String> payloads(PolledMessages polled) {
        return polled.messages().stream()
                .map(message -> new String(message.payload(), StandardCharsets.UTF_8))
                .toList();
    }
}
