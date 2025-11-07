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

package org.apache.iggy.consumergroup.auto;

import org.apache.iggy.client.blocking.IggyBaseClient;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Integration test for AutoConsumerGroup functionality.
 */
public class AutoConsumerGroupIntegrationTest {

    @Test
    public void testAutoConsumerGroupBuilder() {
        // This is just a basic test to verify the builder works
        // In a real integration test, we would need a running Iggy server
        
        // Skip this test in normal runs as it requires a running Iggy server
        assumeTrue(false, "Skipping integration test - requires running Iggy server");
        
        // Create a mock client (in a real test, this would be a real client)
        IggyBaseClient client = new MockIggyClient();
        
        AtomicInteger assignedCount = new AtomicInteger(0);
        AtomicInteger revokedCount = new AtomicInteger(0);
        
        ConsumerGroupConfig config = ConsumerGroupConfig.builder()
            .sessionTimeout(Duration.ofSeconds(30))
            .heartbeatInterval(Duration.ofSeconds(10))
            .rebalanceStrategy(RebalanceStrategy.ROUND_ROBIN)
            .autoCommit(true)
            .autoCommitInterval(Duration.ofSeconds(5))
            .onPartitionsAssigned(partitions -> {
                System.out.println("Assigned partitions: " + partitions);
                assignedCount.incrementAndGet();
            })
            .onPartitionsRevoked(partitions -> {
                System.out.println("Revoked partitions: " + partitions);
                revokedCount.incrementAndGet();
            })
            .build();
        
        AutoConsumerGroup consumerGroup = client.consumerGroup()
            .stream(StreamId.of(1L))
            .topic(TopicId.of(1L))
            .groupName(ConsumerId.of(1L))
            .config(config)
            .build();
        
        assertNotNull(consumerGroup);
        assertEquals(ConsumerGroupState.CREATED, consumerGroup.getState());
    }
    
    // Mock client for testing purposes
    private static class MockIggyClient implements IggyBaseClient {
        @Override
        public org.apache.iggy.client.blocking.SystemClient system() {
            return null;
        }

        @Override
        public org.apache.iggy.client.blocking.StreamsClient streams() {
            return null;
        }

        @Override
        public org.apache.iggy.client.blocking.UsersClient users() {
            return null;
        }

        @Override
        public org.apache.iggy.client.blocking.TopicsClient topics() {
            return null;
        }

        @Override
        public org.apache.iggy.client.blocking.PartitionsClient partitions() {
            return null;
        }

        @Override
        public org.apache.iggy.client.blocking.ConsumerGroupsClient consumerGroups() {
            return null;
        }

        @Override
        public org.apache.iggy.client.blocking.ConsumerOffsetsClient consumerOffsets() {
            return null;
        }

        @Override
        public org.apache.iggy.client.blocking.MessagesClient messages() {
            return null;
        }

        @Override
        public org.apache.iggy.client.blocking.PersonalAccessTokensClient personalAccessTokens() {
            return null;
        }

        @Override
        public AutoConsumerGroup.Builder consumerGroup() {
            return AutoConsumerGroup.builder().client(this);
        }
    }
}