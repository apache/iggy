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
import org.apache.iggy.consumergroup.ConsumerGroupDetails;
import org.apache.iggy.identifier.ConsumerId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.iggy.TestConstants.STREAM_NAME;
import static org.apache.iggy.TestConstants.TOPIC_NAME;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class ConsumerGroupsClientBaseTest extends IntegrationTest {

    protected ConsumerGroupsClient consumerGroupsClient;

    @BeforeEach
    void beforeEachBase() {
        consumerGroupsClient = client.consumerGroups();

        login();
    }

    @Test
    void shouldCreateAndDeleteConsumerGroup() {
        // given
        setUpStreamAndTopic();

        // when
        String consumerGroupName = "consumer-group-42";
        ConsumerGroupDetails consumerGroup = createConsumerGroup(consumerGroupName);

        var consumerGroupById =
                consumerGroupsClient.getConsumerGroup(STREAM_NAME, TOPIC_NAME, ConsumerId.of(consumerGroup.id()));
        var consumerGroupByName =
                consumerGroupsClient.getConsumerGroup(STREAM_NAME, TOPIC_NAME, ConsumerId.of(consumerGroupName));

        // then
        assertThat(consumerGroupById).isPresent();
        assertThat(consumerGroupById.get().id()).isEqualTo(consumerGroup.id());
        assertThat(consumerGroupById.get().name()).isEqualTo(consumerGroupName);
        assertThat(consumerGroupById).isEqualTo(consumerGroupByName);

        // when
        consumerGroupsClient.deleteConsumerGroup(STREAM_NAME, TOPIC_NAME, ConsumerId.of(consumerGroup.id()));

        // then
        assertThat(consumerGroupsClient.getConsumerGroups(STREAM_NAME, TOPIC_NAME))
                .isEmpty();
        assertThat(consumerGroupsClient.getConsumerGroup(STREAM_NAME, TOPIC_NAME, ConsumerId.of(consumerGroup.id())))
                .isEmpty();
    }

    @Test
    void shouldDeleteConsumerGroupByName() {
        // given
        setUpStreamAndTopic();
        String groupName = "consumes-group-42";
        createConsumerGroup(groupName);
        var consumerGroup = consumerGroupsClient.getConsumerGroup(STREAM_NAME, TOPIC_NAME, ConsumerId.of(groupName));
        assert consumerGroup.isPresent();

        // when
        consumerGroupsClient.deleteConsumerGroup(STREAM_NAME, TOPIC_NAME, ConsumerId.of(groupName));

        // then
        assertThat(consumerGroupsClient.getConsumerGroups(STREAM_NAME, TOPIC_NAME))
                .isEmpty();
    }

    @Test
    void shouldGetAllConsumerGroups() {
        // given
        setUpStreamAndTopic();

        createConsumerGroup("consumer-group-42");
        createConsumerGroup("consumer-group-43");
        createConsumerGroup("consumer-group-44");

        // when
        var consumerGroups = consumerGroupsClient.getConsumerGroups(STREAM_NAME, TOPIC_NAME);

        // then
        assertThat(consumerGroups).hasSize(3);
        assertThat(consumerGroups)
                .map(ConsumerGroup::name)
                .containsExactlyInAnyOrder("consumer-group-42", "consumer-group-43", "consumer-group-44");
    }

    private ConsumerGroupDetails createConsumerGroup(String groupName) {
        return consumerGroupsClient.createConsumerGroup(STREAM_NAME, TOPIC_NAME, groupName);
    }
}
