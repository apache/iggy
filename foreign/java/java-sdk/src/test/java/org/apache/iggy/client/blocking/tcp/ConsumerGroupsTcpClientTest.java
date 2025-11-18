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
import org.apache.iggy.identifier.ConsumerId;
import org.junit.jupiter.api.Test;

import static org.apache.iggy.TestConstants.STREAM_NAME;
import static org.apache.iggy.TestConstants.TOPIC_NAME;
import static org.assertj.core.api.Assertions.assertThat;

class ConsumerGroupsTcpClientTest extends ConsumerGroupsClientBaseTest {

    @Override
    protected IggyBaseClient getClient() {
        return TcpClientFactory.create(iggyServer);
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
}
