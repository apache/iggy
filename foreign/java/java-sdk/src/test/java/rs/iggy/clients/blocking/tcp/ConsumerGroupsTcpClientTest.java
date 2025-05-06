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

package rs.iggy.clients.blocking.tcp;

import org.junit.jupiter.api.Test;
import rs.iggy.clients.blocking.ConsumerGroupsClientBaseTest;
import rs.iggy.clients.blocking.IggyBaseClient;
import java.util.Optional;
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
        var group = consumerGroupsClient.createConsumerGroup(42L,
                42L,
                Optional.of(42L),
                "consumer-group-42");

        // when
        consumerGroupsClient.joinConsumerGroup(42L, 42L, group.id());

        // then
        group = consumerGroupsClient.getConsumerGroup(42L, 42L, group.id()).get();
        assertThat(group.membersCount()).isEqualTo(1);

        // when
        consumerGroupsClient.leaveConsumerGroup(42L, 42L, group.id());

        // then
        group = consumerGroupsClient.getConsumerGroup(42L, 42L, group.id()).get();
        assertThat(group.membersCount()).isEqualTo(0);
    }

}
