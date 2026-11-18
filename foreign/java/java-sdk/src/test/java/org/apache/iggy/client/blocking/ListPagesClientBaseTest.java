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

package org.apache.iggy.client.blocking;

import org.apache.iggy.consumergroup.ConsumerGroup;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyValidationException;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.partition.Partition;
import org.apache.iggy.stream.StreamBase;
import org.apache.iggy.system.NamedPageQuery;
import org.apache.iggy.system.Page;
import org.apache.iggy.system.PageCursor;
import org.apache.iggy.system.PageOrder;
import org.apache.iggy.system.PageQuery;
import org.apache.iggy.topic.Topic;
import org.apache.iggy.user.UserInfo;
import org.apache.iggy.user.UserStatus;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import static org.apache.iggy.TestConstants.PERSISTED_TOPIC_OPTIONS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The page methods of a blocking client against a live server. */
public abstract class ListPagesClientBaseTest extends IntegrationTest {

    /** A page cursor of a kind that no server writes. The client sends it and the server refuses it. */
    private static final String UNKNOWN_KIND_CURSOR = "pg1_AwcAAAAAAAAA";

    private static final long WALK_LIMIT = 2;
    private static final long PARTITIONS = 3;
    private static final String PASSWORD = "list-pages-password";

    @BeforeEach
    void beforeEachListPagesTest() {
        login();
    }

    @Test
    void givenThreeStreamsWhenWalkingTheirPagesShouldReturnThemInTheOrderOfTheQuery() {
        String prefix = unique("pages-walk");
        List<Long> created = new ArrayList<>();
        for (String suffix : List.of("-a", "-b", "-c")) {
            long id = client.streams().createStream(prefix + suffix).id();
            trackStream(id);
            created.add(id);
        }
        assertThat(walk(PageOrder.ID, false, prefix))
                .isEqualTo(created.stream().sorted().toList());
        assertThat(walk(PageOrder.ID, true, prefix))
                .isEqualTo(created.stream().sorted(Comparator.reverseOrder()).toList());
        assertThat(walk(PageOrder.CREATED, false, prefix)).isEqualTo(created);
        assertThat(client.streams().getStreams()).extracting(StreamBase::id).containsAll(created);
    }

    @Test
    void givenATopicWithThreePartitionsWhenReadingPagesShouldReturnEveryPartition() {
        StreamId stream = StreamId.of(createStream(unique("pages-partitions")));
        TopicId topic = TopicId.of(client.topics()
                .createTopic(stream, "events", PARTITIONS, PERSISTED_TOPIC_OPTIONS)
                .id());
        PageQuery query = PageQuery.builder().limit(WALK_LIMIT).build();

        Page<Partition> first = client.partitions().getPartitionsPage(stream, topic, query);
        Page<Partition> second = client.partitions()
                .getPartitionsPage(stream, topic, query.withCursor(first.next().orElseThrow()));

        assertThat(first.items()).extracting(Partition::id).containsExactly(0L, 1L);
        assertThat(second.items()).extracting(Partition::id).containsExactly(2L);
        assertThat(second.next()).isEmpty();
    }

    @Test
    void givenANameFilterWhenReadingPagesShouldKeepOnlyTheMatchingEntries() {
        StreamId stream = StreamId.of(createStream(unique("pages-names")));
        TopicId topic = TopicId.of(client.topics()
                .createTopic(stream, "sensor-readings", 1L, PERSISTED_TOPIC_OPTIONS)
                .id());
        client.topics().createTopic(stream, "events", 1L, PERSISTED_TOPIC_OPTIONS);
        client.consumerGroups().createConsumerGroup(stream, topic, "billing");
        client.consumerGroups().createConsumerGroup(stream, topic, "audit");
        String username = unique("pages-user");
        trackUser(client.users()
                .createUser(username, PASSWORD, UserStatus.Active, Optional.empty())
                .id());

        Page<Topic> topics = client.topics().getTopicsPage(stream, named("sensor"));
        Page<ConsumerGroup> groups = client.consumerGroups().getConsumerGroupsPage(stream, topic, named("bill"));
        Page<UserInfo> users = client.users().getUsersPage(named(username));

        assertThat(topics.items()).extracting(Topic::name).containsExactly("sensor-readings");
        assertThat(groups.items()).extracting(ConsumerGroup::name).containsExactly("billing");
        assertThat(users.items()).extracting(UserInfo::username).containsExactly(username);
        assertThat(List.of(topics.next(), groups.next(), users.next())).containsOnly(Optional.empty());
    }

    @Test
    void givenQueriesTheServerCannotServeWhenReadingPagesShouldRefuseThemAndStayUsable() {
        StreamId stream = StreamId.of(createStream(unique("pages-refused")));
        TopicId topic = TopicId.of(client.topics()
                .createTopic(stream, "events", 1L, PERSISTED_TOPIC_OPTIONS)
                .id());

        assertRefused(() -> client.streams()
                .getStreamsPage(new NamedPageQuery(PageQuery.builder()
                        .cursor(PageCursor.parse(UNKNOWN_KIND_CURSOR))
                        .build())));

        assertThat(client.partitions()
                        .getPartitionsPage(stream, topic, PageQuery.builder().build())
                        .items())
                .extracting(Partition::id)
                .containsExactly(0L);
    }

    /** Reads the streams that hold {@code name} in pages of two and checks that the second page is the last. */
    private List<Long> walk(PageOrder order, boolean descending, String name) {
        NamedPageQuery query = new NamedPageQuery(
                PageQuery.builder()
                        .order(order)
                        .descending(descending)
                        .limit(WALK_LIMIT)
                        .build(),
                Optional.of(name));
        Page<StreamBase> first = client.streams().getStreamsPage(query);
        Page<StreamBase> second =
                client.streams().getStreamsPage(query.withCursor(first.next().orElseThrow()));

        assertThat(first.items()).hasSize((int) WALK_LIMIT);
        assertThat(second.next()).isEmpty();
        return Stream.concat(first.items().stream(), second.items().stream())
                .map(StreamBase::id)
                .toList();
    }

    private long createStream(String name) {
        long id = client.streams().createStream(name).id();
        trackStream(id);
        return id;
    }

    private static NamedPageQuery named(String name) {
        return new NamedPageQuery(PageQuery.builder().build(), Optional.of(name));
    }

    private static String unique(String prefix) {
        return prefix + "-" + UUID.randomUUID();
    }

    private static void assertRefused(ThrowingCallable call) {
        assertThatThrownBy(call)
                .isInstanceOfSatisfying(
                        IggyValidationException.class,
                        refusal -> assertThat(refusal.getErrorCode()).isEqualTo(IggyErrorCode.INVALID_COMMAND));
    }
}
