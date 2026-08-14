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
import org.apache.iggy.exception.IggyResourceNotFoundException;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;
import org.apache.iggy.message.PollingStrategy;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.apache.iggy.TestConstants.STREAM_NAME;
import static org.apache.iggy.TestConstants.TOPIC_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConsumerGroupsTcpClientTest extends ConsumerGroupsClientBaseTest {

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
    void shouldSyncConsumerGroupAssignmentAfterJoin() {
        // given
        setUpStreamAndTopic();
        var group = consumerGroupsClient.createConsumerGroup(STREAM_NAME, TOPIC_NAME, "consumer-group-42");
        ConsumerId groupId = ConsumerId.of(group.id());

        // when
        consumerGroupsClient.joinConsumerGroup(STREAM_NAME, TOPIC_NAME, groupId);
        var assignment = consumerGroupsClient.syncConsumerGroup(STREAM_NAME, TOPIC_NAME, groupId);

        // then — the only member owns the topic's single partition
        assertThat(assignment).isPresent();
        assertThat(assignment.get().partitions()).hasSize(1);
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
    void shouldPollAsGroupMemberWithoutExplicitPartition() {
        // given
        setUpStreamAndTopic();
        var group = consumerGroupsClient.createConsumerGroup(STREAM_NAME, TOPIC_NAME, "consumer-group-42");
        ConsumerId groupId = ConsumerId.of(group.id());
        consumerGroupsClient.joinConsumerGroup(STREAM_NAME, TOPIC_NAME, groupId);
        client.messages()
                .sendMessages(
                        STREAM_NAME, TOPIC_NAME, Partitioning.partitionId(0L), List.of(Message.of("group message")));

        // when — the partition is selected client-side from the synced assignment
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
}
