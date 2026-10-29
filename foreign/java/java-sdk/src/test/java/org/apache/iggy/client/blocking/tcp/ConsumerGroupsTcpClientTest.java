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

import org.apache.iggy.client.blocking.ConsumerGroupsClientBaseTest;
import org.apache.iggy.client.blocking.IggyBaseClient;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.consumergroup.ConsumerGroupAssignment;
import org.apache.iggy.exception.IggyResourceNotFoundException;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.message.DeferredPollOptions;
import org.apache.iggy.message.FetchedMessages;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.iggy.TestConstants.PERSISTED_TOPIC_OPTIONS;
import static org.apache.iggy.TestConstants.STREAM_NAME;
import static org.apache.iggy.TestConstants.TOPIC_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConsumerGroupsTcpClientTest extends ConsumerGroupsClientBaseTest {

    private static final Duration ASSIGNMENT_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration ASSIGNMENT_POLL_INTERVAL = Duration.ofMillis(10);
    private static final int GROUP_PARTITIONS = 3;
    private static final long READY_PARTITION = 2L;
    private static final long BACKLOG_PARTITION = 0L;
    private static final Duration LONG_WAIT = Duration.ofSeconds(10);
    // Far below the wait, so a fetch that waited for its whole wait fails it.
    private static final Duration READY_FETCH_BOUND = Duration.ofSeconds(2);
    private static final Duration BACKLOG_FETCH_PAUSE = Duration.ofMillis(100);
    // More than the fetches that fit in the bound, so the backlog lasts for the whole test.
    private static final int BACKLOG_MESSAGES = 50;

    @Override
    protected IggyBaseClient getClient() {
        return TcpClientFactory.create(serverHost(), serverTcpPort());
    }

    @Test
    void shouldJoinAndLeaveConsumerGroup() {
        // given
        setUpStreamAndTopic();
        var group = consumerGroupsClient.createConsumerGroup(STREAM_NAME, TOPIC_NAME, "consumer-group-42");
        ConsumerId groupId = ConsumerId.of(group.id());

        // when
        consumerGroupsClient.joinConsumerGroup(STREAM_NAME, TOPIC_NAME, groupId);

        // then
        group = consumerGroupsClient
                .getConsumerGroup(STREAM_NAME, TOPIC_NAME, groupId)
                .get();
        assertThat(group.membersCount()).isEqualTo(1);

        // when
        consumerGroupsClient.leaveConsumerGroup(STREAM_NAME, TOPIC_NAME, groupId);

        // then
        group = consumerGroupsClient
                .getConsumerGroup(STREAM_NAME, TOPIC_NAME, groupId)
                .get();
        assertThat(group.membersCount()).isEqualTo(0);
    }

    @Test
    void shouldSyncConsumerGroupAssignmentAfterJoin() throws InterruptedException {
        // given
        setUpStreamAndTopic();
        var group = consumerGroupsClient.createConsumerGroup(STREAM_NAME, TOPIC_NAME, "consumer-group-42");
        ConsumerId groupId = ConsumerId.of(group.id());

        // when
        consumerGroupsClient.joinConsumerGroup(STREAM_NAME, TOPIC_NAME, groupId);
        var assignment = awaitPartitionAssignment(groupId, 1);

        assertThat(assignment.partitions()).hasSize(1);
    }

    @Test
    void shouldReturnEmptyAssignmentWhenNotMember() {
        // given
        setUpStreamAndTopic();
        var group = consumerGroupsClient.createConsumerGroup(STREAM_NAME, TOPIC_NAME, "consumer-group-42");
        ConsumerId groupId = ConsumerId.of(group.id());

        // when
        var assignment = consumerGroupsClient.syncConsumerGroup(STREAM_NAME, TOPIC_NAME, groupId);

        // then
        assertThat(assignment).isEmpty();
    }

    @Test
    void shouldPollAsGroupMemberWithoutExplicitPartition() throws InterruptedException {
        // given
        setUpStreamAndTopic();
        var group = consumerGroupsClient.createConsumerGroup(STREAM_NAME, TOPIC_NAME, "consumer-group-42");
        ConsumerId groupId = ConsumerId.of(group.id());
        consumerGroupsClient.joinConsumerGroup(STREAM_NAME, TOPIC_NAME, groupId);
        awaitPartitionAssignment(groupId, 1);
        client.messages()
                .sendMessages(
                        STREAM_NAME, TOPIC_NAME, Partitioning.partitionId(0L), List.of(Message.of("group message")));

        var polledMessages = client.messages()
                .pollMessages(
                        STREAM_NAME,
                        TOPIC_NAME,
                        Optional.empty(),
                        Consumer.group(groupId),
                        PollingStrategy.first(),
                        10L,
                        false);

        // then
        assertThat(polledMessages.messages()).hasSize(1);
        assertThat(new String(polledMessages.messages().get(0).payload())).isEqualTo("group message");
    }

    @Test
    void shouldFailGroupPollWhenNotJoined() {
        // given
        setUpStreamAndTopic();
        var group = consumerGroupsClient.createConsumerGroup(STREAM_NAME, TOPIC_NAME, "consumer-group-42");
        ConsumerId groupId = ConsumerId.of(group.id());

        // when / then
        assertThatThrownBy(() -> client.messages()
                        .pollMessages(
                                STREAM_NAME,
                                TOPIC_NAME,
                                Optional.empty(),
                                Consumer.group(groupId),
                                PollingStrategy.first(),
                                10L,
                                false))
                .isInstanceOf(IggyResourceNotFoundException.class);
    }

    /**
     * A member owns several partitions, and only one of them gets messages. One fetch waits on all of them, so it
     * returns once that partition holds a message, long before the wait ends.
     */
    @Test
    void shouldFetchThePartitionWithMessagesOfAMemberOfSeveralPartitions() throws InterruptedException {
        ConsumerId groupId = joinGroupOfSeveralPartitions();
        var options = DeferredPollOptions.defaults().withMaxWait(LONG_WAIT);

        for (int round = 0; round < GROUP_PARTITIONS; round++) {
            String payload = "ready " + round;
            client.messages()
                    .sendMessages(
                            STREAM_NAME,
                            TOPIC_NAME,
                            Partitioning.partitionId(READY_PARTITION),
                            List.of(Message.of(payload)));
            long started = System.nanoTime();
            var sections = fetchGroup(groupId, groupTargets(), options);

            assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(READY_FETCH_BOUND);
            assertThat(served(sections)).singleElement().satisfies(section -> {
                assertThat(section.partitionId()).isEqualTo(READY_PARTITION);
                assertThat(payloads(section.decode())).containsExactly(payload);
            });
        }
    }

    /**
     * One partition of a member keeps a backlog, so it holds messages at every fetch. A fetch that puts the served
     * partitions last still reads another partition that gets a message.
     */
    @Test
    void shouldFetchAPartitionThatGetsAMessageWhileAnotherKeepsABacklog() throws InterruptedException {
        ConsumerId groupId = joinGroupOfSeveralPartitions();
        client.messages()
                .sendMessages(
                        STREAM_NAME,
                        TOPIC_NAME,
                        Partitioning.partitionId(BACKLOG_PARTITION),
                        Collections.nCopies(BACKLOG_MESSAGES, Message.of("backlog")));
        var options = DeferredPollOptions.defaults();
        Map<Long, PollingStrategy> targets = groupTargets();
        var sections = fetchGroup(groupId, targets, options);
        assertThat(served(sections)).extracting(FetchedMessages::partitionId).containsExactly(BACKLOG_PARTITION);

        client.messages()
                .sendMessages(
                        STREAM_NAME,
                        TOPIC_NAME,
                        Partitioning.partitionId(READY_PARTITION),
                        List.of(Message.of("late")));
        long deadline = System.nanoTime() + READY_FETCH_BOUND.toNanos();
        Optional<FetchedMessages> late;
        do {
            Thread.sleep(BACKLOG_FETCH_PAUSE.toMillis());
            targets = nextTargets(targets, sections);
            sections = fetchGroup(groupId, targets, options);
            late = served(sections).stream()
                    .filter(section -> section.partitionId() == READY_PARTITION)
                    .findFirst();
        } while (late.isEmpty() && System.nanoTime() < deadline);

        assertThat(late)
                .hasValueSatisfying(
                        section -> assertThat(payloads(section.decode())).containsExactly("late"));
    }

    private ConsumerId joinGroupOfSeveralPartitions() throws InterruptedException {
        setUpStream();
        client.topics().createTopic(STREAM_NAME, TOPIC_NAME.getName(), GROUP_PARTITIONS, PERSISTED_TOPIC_OPTIONS);
        var group = consumerGroupsClient.createConsumerGroup(STREAM_NAME, TOPIC_NAME, "consumer-group-42");
        ConsumerId groupId = ConsumerId.of(group.id());
        consumerGroupsClient.joinConsumerGroup(STREAM_NAME, TOPIC_NAME, groupId);
        awaitPartitionAssignment(groupId, GROUP_PARTITIONS);
        return groupId;
    }

    /** Every partition of the topic at its next offset. The member owns them all, and one server is their primary. */
    private static Map<Long, PollingStrategy> groupTargets() {
        Map<Long, PollingStrategy> targets = new LinkedHashMap<>();
        for (long partition = 0; partition < GROUP_PARTITIONS; partition++) {
            targets.put(partition, PollingStrategy.next());
        }
        return targets;
    }

    /** A fetch of at most one message that commits what it reads. */
    private List<FetchedMessages> fetchGroup(
            ConsumerId groupId, Map<Long, PollingStrategy> targets, DeferredPollOptions options) {
        var sections = client.messages()
                .fetchMessages(STREAM_NAME, TOPIC_NAME, targets, Consumer.group(groupId), 1L, true, options);
        assertThat(sections).allSatisfy(section -> assertThat(section.status()).isEqualTo(FetchedMessages.STATUS_OK));
        return sections;
    }

    private static List<FetchedMessages> served(List<FetchedMessages> sections) {
        return sections.stream().filter(section -> section.count() > 0).toList();
    }

    /** Each served partition continues at its next offset, with the fence of its section, after the other targets. */
    private static Map<Long, PollingStrategy> nextTargets(
            Map<Long, PollingStrategy> sent, List<FetchedMessages> sections) {
        Map<Long, PollingStrategy> next = new LinkedHashMap<>(sent);
        for (FetchedMessages section : served(sections)) {
            next.remove(section.partitionId());
            next.put(
                    section.partitionId(),
                    PollingStrategy.offset(section.nextOffset()).withContext(section.context()));
        }
        return next;
    }

    private static List<String> payloads(PolledMessages polled) {
        return polled.messages().stream()
                .map(message -> new String(message.payload(), StandardCharsets.UTF_8))
                .toList();
    }

    private ConsumerGroupAssignment awaitPartitionAssignment(ConsumerId groupId, int partitions)
            throws InterruptedException {
        long deadline = System.nanoTime() + ASSIGNMENT_TIMEOUT.toNanos();
        Optional<ConsumerGroupAssignment> assignment;
        do {
            assignment = consumerGroupsClient.syncConsumerGroup(STREAM_NAME, TOPIC_NAME, groupId);
            if (assignment.isPresent() && assignment.get().partitions().size() >= partitions) {
                return assignment.get();
            }
            Thread.sleep(ASSIGNMENT_POLL_INTERVAL.toMillis());
        } while (System.nanoTime() < deadline);
        throw new AssertionError("Consumer-group ownership was not installed: " + assignment);
    }
}
