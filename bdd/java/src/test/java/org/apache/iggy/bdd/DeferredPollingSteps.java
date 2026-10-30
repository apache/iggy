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
import org.apache.iggy.client.async.reader.IggyConsumer;
import org.apache.iggy.client.async.reader.PartitionReader;
import org.apache.iggy.client.async.reader.ReceivedMessage;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.CommitPolicy;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PollOptions;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
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
    private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(30);

    private AsyncIggyTcpClient client;
    private Long streamId;
    private String topicName;
    private CompletableFuture<PolledMessages> poll;
    private long pollStartedAtNanos;
    private PartitionReader reader;
    private IggyConsumer consumer;

    @After
    public void close() throws InterruptedException, ExecutionException, TimeoutException {
        if (client == null) {
            return;
        }
        try {
            if (poll != null) {
                poll.cancel(true);
            }
            if (reader != null) {
                reader.closeAsync().get(CLOSE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
            closeConsumer();
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
    }

    @When("I start a deferred poll of partition {int} from offset {int} with a max wait of {int} second(s)")
    public void startOffsetPoll(int partition, int offset, int maxWaitSeconds) {
        startPoll(partition, DEFAULT_CONSUMER, offset(offset), waitFor(maxWaitSeconds));
    }

    @When(
            "I start a deferred poll of partition {int} for consumer {string} with the on read commit policy, a min count of {int} and a max wait of {int} second(s)")
    public void startCommittingPoll(int partition, String consumerName, int minCount, int maxWaitSeconds) {
        startPoll(
                partition,
                Consumer.of(ConsumerId.of(consumerName)),
                PollingStrategy.next(),
                waitFor(maxWaitSeconds).withMinCount(minCount).withCommit(CommitPolicy.onRead()));
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

    @Then(
            "a poll of partition {int} for consumer {string} with the next strategy and the on read commit policy should return the payload {string}")
    public void committingPollReturnsPayload(int partition, String consumerName, String payload) {
        assertEquals(
                List.of(payload),
                payloads(onePoll(
                        partition,
                        consumerName,
                        PollOptions.defaults().withCount(1).withCommit(CommitPolicy.onRead()))));
    }

    @Then("a poll of partition {int} for consumer {string} with the next strategy should return the payload {string}")
    public void pollOfNextReturnsPayload(int partition, String consumerName, String payload) {
        PolledMessages polled = onePoll(partition, consumerName, PollOptions.defaults());

        assertFalse(polled.messages().isEmpty(), "The poll read no message");
        assertEquals(payload, payloads(polled).get(0));
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

    @When(
            "I open a partition reader on partitions {int}, {int} and {int} from offset {int} with a max wait of {int} second(s)")
    public void openReader(int first, int second, int third, int offset, int maxWaitSeconds)
            throws InterruptedException, ExecutionException, TimeoutException {
        reader = client().reader(stream(), topic(), DEFAULT_CONSUMER)
                .partitions(List.of((long) first, (long) second, (long) third))
                .start(offset(offset))
                .options(waitFor(maxWaitSeconds))
                .build()
                .get(CLOSE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Then("the partition reader should return the payload {string} from partition {int} within {int} second(s)")
    public void readerReturnsPayload(String payload, int partition, int seconds)
            throws InterruptedException, ExecutionException {
        PolledMessages batch = within(reader.next(), Duration.ofSeconds(seconds));

        assertEquals(partition, batch.partitionId());
        assertEquals(List.of(payload), payloads(batch));
    }

    @When("I open a consumer in group {string} on the topic with a max wait of {int} second(s)")
    public void openGroupConsumer(String group, int maxWaitSeconds)
            throws InterruptedException, ExecutionException, TimeoutException {
        openConsumer(group, waitFor(maxWaitSeconds));
    }

    @When("I open a consumer in group {string} on the topic that commits after processing")
    public void openProcessingConsumer(String group) throws InterruptedException, ExecutionException, TimeoutException {
        openConsumer(group, PollOptions.waitFor(Duration.ofSeconds(5)).withCommit(CommitPolicy.auto()));
    }

    @Then("the consumer should return the payload {string} within {int} second(s)")
    public void consumerReturnsPayload(String payload, int seconds) throws InterruptedException, ExecutionException {
        ReceivedMessage message = within(consumer.next(), Duration.ofSeconds(seconds));

        assertEquals(payload, new String(message.message().payload(), StandardCharsets.UTF_8));
    }

    @When("I close the consumer")
    public void closeConsumer() throws InterruptedException, ExecutionException, TimeoutException {
        if (consumer != null) {
            IggyConsumer closing = consumer;
            consumer = null;
            closing.shutdown().get(CLOSE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    private void openConsumer(String group, PollOptions options)
            throws InterruptedException, ExecutionException, TimeoutException {
        ConsumerId id = ConsumerId.of(group);
        if (client().consumerGroups()
                .getConsumerGroup(stream(), topic(), id)
                .join()
                .isEmpty()) {
            client().consumerGroups()
                    .createConsumerGroup(stream(), topic(), group)
                    .join();
        }
        consumer = IggyConsumer.build(
                        client().reader(stream(), topic(), Consumer.group(id)).options(options))
                .get(CLOSE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    private PolledMessages onePoll(int partition, String consumerName, PollOptions options) {
        return client().messages()
                .pollMessages(
                        stream(),
                        topic(),
                        partition,
                        Consumer.of(ConsumerId.of(consumerName)),
                        PollingStrategy.next(),
                        options)
                .join();
    }

    private void startPoll(int partition, Consumer pollConsumer, PollingStrategy strategy, PollOptions options) {
        pollStartedAtNanos = System.nanoTime();
        poll = client().messages().pollMessages(stream(), topic(), partition, pollConsumer, strategy, options);
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

    private static PollOptions waitFor(int maxWaitSeconds) {
        return PollOptions.waitFor(Duration.ofSeconds(maxWaitSeconds));
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
