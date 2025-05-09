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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.apache.iggy.consumergroup.ConsumerGroup;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import java.util.Optional;
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
        consumerGroupsClient.createConsumerGroup(42L,
                42L,
                Optional.of(42L),
                "consumer-group-42");

        var consumerGroupById = consumerGroupsClient.getConsumerGroup(StreamId.of(42L),
                TopicId.of(42L),
                ConsumerId.of(42L));
        var consumerGroupByName = consumerGroupsClient.getConsumerGroup(StreamId.of(42L),
                TopicId.of(42L),
                ConsumerId.of("consumer-group-42"));

        // then
        assertThat(consumerGroupById).isPresent();
        assertThat(consumerGroupById.get().id()).isEqualTo(42L);
        assertThat(consumerGroupById.get().name()).isEqualTo("consumer-group-42");
        assertThat(consumerGroupById).isEqualTo(consumerGroupByName);

        // when
        consumerGroupsClient.deleteConsumerGroup(42L, 42L, 42L);

        // then
        assertThat(consumerGroupsClient.getConsumerGroups(42L, 42L)).isEmpty();
        assertThat(consumerGroupsClient.getConsumerGroup(42L, 42L, 42L)).isEmpty();
    }

    @Test
    void shouldDeleteConsumerGroupByName() {
        // given
        setUpStreamAndTopic();
        consumerGroupsClient.createConsumerGroup(42L, 42L, Optional.of(42L), "consumer-group-42");
        var consumerGroup = consumerGroupsClient.getConsumerGroup(42L, 42L, 42L);
        assert consumerGroup.isPresent();

        // when
        consumerGroupsClient.deleteConsumerGroup(StreamId.of(42L), TopicId.of(42L),
                ConsumerId.of("consumer-group-42"));

        // then
        assertThat(consumerGroupsClient.getConsumerGroups(42L, 42L)).isEmpty();
    }

    @Test
    void shouldGetAllConsumerGroups() {
        // given
        setUpStreamAndTopic();

        consumerGroupsClient.createConsumerGroup(42L, 42L, Optional.of(42L), "consumer-group-42");
        consumerGroupsClient.createConsumerGroup(42L, 42L, Optional.of(43L), "consumer-group-43");
        consumerGroupsClient.createConsumerGroup(42L, 42L, Optional.of(44L), "consumer-group-44");

        // when
        var consumerGroups = consumerGroupsClient.getConsumerGroups(42L, 42L);

        // then
        assertThat(consumerGroups).hasSize(3);
        assertThat(consumerGroups)
                .map(ConsumerGroup::name)
                .containsExactlyInAnyOrder("consumer-group-42", "consumer-group-43", "consumer-group-44");
    }

}
